package Fork::ParallelJob;

use POSIX ':sys_wait_h', 'setsid';
use Class::Load qw/load_class/;
use JSON::XS;
use Fcntl qw(:flock);
use Time::HiRes qw/sleep/;
use strict;
use warnings;

sub new {
  my $class = shift;
  my $self = bless
    {
     name               => '',
     pid                => $$,
     close              => 0,
     max_process        => 0,
     retry              => 0,
     setsid             => 0,
     pid                => $$,
     data_format        => 'Storable',
     parent_data_format => '',
     child_data_format  => '',
     wait_sleep         => 0.5,
     jobs_in_data       => 0,
     @_,
     pids       => [],
     count      => 0,
     current    => 0,
    }, $class;
  $self->{name}      ||= $$;
  $self->{parent_data_format} = 'Storable'  if $self->{jobs_in_data};
  $self->{jobs} = [];
  my $parent_data_class = 'Fork::ParallelJob::Data::' . ($self->{parent_data_format} || $self->{data_format});
  my $child_data_class  = 'Fork::ParallelJob::Data::' . ($self->{child_data_format}  || $self->{data_format});
  load_class($parent_data_class);
  load_class($child_data_class);
  $self->{'child_data'}  ||= $parent_data_class->new(base_dir => "/tmp/fork-child-"  . $self->{'name'});
  $self->{'parent_data'} ||= $child_data_class->new(base_dir => "/tmp/fork-parent-" . $self->{'name'});
  if (! $self->{is_child}) {
    $self->parent_data->set_worker_id($self->{'name'});
  }
  return $self;
}

sub child {
  my ($self) = shift;
  my $o = (ref $self)->new(%$self, @_, is_child => 1);
  return $o;
}

sub add_job {
  my ($self, $job, $d) = @_;
  if ($self->{jobs_in_data}) {
    my $pd = $self->parent_data;
    $pd->lock(sub {
                my $data = $pd->get;
                my $jobs = $data->{jobs} ||= [];
                my ($name, $job_code) = ref $job eq 'CODE' ? (scalar @$jobs + 1, $job) : (%$job);
                push @$jobs, {$name, $job_code};
                $data->{jobs} = $jobs;
                $pd->set($data);
              });
  } else {
    my $jobs = $self->{jobs} ||= [];
    my $data = $self->{data} ||= [];
    my ($name, $job_code) = ref $job eq 'CODE' ? (scalar @$jobs + 1, $job) : (%$job);
    $data->[scalar @$jobs] = $d if $d;
    push @$jobs, {$name, $job_code};
  }
}

sub take_job {
  my ($self) = @_;
  if ($self->{jobs_in_data}) {
    my $pd = $self->parent_data;
    my $job;
    my $data;
    $pd->lock(sub { $data = $pd->get; $job = shift @{$data->{jobs} ||= []}; $pd->set($data) });
    return $job ? $job : ();
  } else {
    shift @{$self->{jobs} || []};
  }
}

sub has_job {
  my ($self) = @_;
  if ($self->{jobs_in_data}) {
    return scalar @{$self->parent_data->get->{jobs} ||= []};
  } else {
    return scalar @{$self->{jobs}};
  }
}

sub do_fork {
  my $self = shift;
  my $check = pop if ref $_[-1] eq 'CODE';
  my $jobs = $self->{jobs} ||= [];
  my $data = $self->{data} ||= [];
  if (@_) {
    my $jobs = shift;
    while (my $job = shift @$jobs) {
      $self->add_job(ref $job eq 'CODE' ? $job : {$job => shift @$jobs});
    }
    @{$data} = @{shift() || []};
  }
  $self->{count}++;
  $self->{current}++;

  my $pids = $self->{pids};
  my ($job_name, $job) = %{$self->take_job || {}};
  my $job_data = shift @{$data ||= []};
  my $retry_fork = $self->{retry_fork};
  $self->{parent} = $self->{name};

 FORK:
  if (my $pid = fork()) {
    # parent
    if ($self->{close}) {
      local *STDIN;
      local *STDOUT;
      local *STDERR;
    }
    $SIG{CHLD} = 'IGNORE'  if $self->{setsid};

    push @$pids, $pid;
    if ($self->{max_process} and ! $self->{setsid}) {
      # ignore check & max_process when setsid is true
      $self->_wait_pids($self->{max_process});
    }
  JOBS:
    {
      if ($self->has_job) {
        $self->do_fork(ref $check eq 'CODE' ? $check : ());
      } else {
        if (! $self->{setsid}) {
          $self->_wait_pids(0, $check);
        }
        if ($self->{is_child}) {
          exit $self->result;
        }
        redo JOBS if $self->has_job
      }
    }
  } elsif (defined $pid) {
    # child
    POSIX::setsid() if $self->{setsid};
    $self->{unique_name} = $self->{parent} ? $self->{parent} . '-' . $self->{count} : $self->{count};
    $self->child_data->set_worker_id($self->{unique_name});
    delete $self->{parent};
    my $retry = $self->{retry};
    my $status;
  EXEC: {
      if ($self->{setsid}) {
        setsid or die "Can't start a new session: $!";
      }
      if ($self->{close}) {
        local *STDIN;
        local *STDOUT;
        local *STDERR;
      }
      my $result = eval {$job->($self, $job_data)};
      if ($@) {
        warn sprintf "(%s) process($$) %s error: %s", $self->{name}, $job_name, $@;
        if ($retry--) {
          warn sprintf "(%s) retry process($$) %s", $self->{name}, $job_name;
          redo EXEC
        }
        $status = 0;
      } else {
        $status = $result;
      }
      my $child_data = $self->child_data->get;
      $child_data->{result} = $status;
      $child_data->{pid}    = $$;
      $self->child_data->set($child_data);
    }
    exit $status;
  } else {
    # error
    redo FORK if $retry_fork-- > 0;

    die "cannot fork $job_name";
  }
}

sub _wait_pids {
  my $self = shift;
  my $limit = shift || 0;
  my $check = shift;
  my $pids = $self->{pids};
  my %pids;
  @pids{@$pids} = ();
  while ($limit ? keys %pids >= $limit : keys %pids) {
    foreach my $pid (keys %pids) {
      my $kill_pid = waitpid $pid, WNOHANG;
      if ($kill_pid > 0 or $kill_pid == -1) {
        $self->{current}--;
        $self->{result}->{$kill_pid} = $? if $kill_pid > 0;
        delete $pids{$pid};
      }
    }
    if ($check) {
      $check->();
      return if $self->has_job;
    }
    sleep $self->{wait_sleep};
  }
}

sub result {
  my $self = shift;
  my $r = 1;
  foreach my $data (@{$self->child_data->get_all}) {
    $r &= $data->{result};
  }
  return $r;
}

sub child_data {
  my $self = shift;
  $self->{child_data}
}

sub parent_data {
  my $self = shift;
  $self->{parent_data}
}

1;

=head1 NAME

Fork::ParallelJob -- simply do jobs parallelly using fork

=head1 SYNOPSIS

  my $now = time;
  my $fork = Fork::ParallelJob->new(max_process => 3, name => "fork1", close => 0, setsid => 0);
  # the following jobs will do with fork
  my @jobs = (
              sub
              {
                # child of child
                my $f = $fork->child(name => "fork2", max_process => 2, retry => 4);
                $f->do_fork([
                             name1 => sub { print "1-1c - $$\n";sleep 4;  print 1,"\t -- \t",time - $now,"\n" },
                             name2 => sub { print "1-2c - $$\n";sleep 5;  print 2,"\t -- \t",time - $now,"\n" },
                             name3 => sub { print "1-3c - $$\n";sleep 6;  print 3,"\t -- \t",time - $now,"\n" },
                            ]);
              }
              sub { print 2,"p - $$\n"; sleep 1;  print 1,"\t",time - $now,"\n" },
              sub { print 3,"p - $$\n"; sleep 2;  print 2,"\t",time - $now,"\n" },
              sub { print 4,"p - $$\n"; sleep 3;  print 3,"\t",time - $now,"\n" },
             );

  my ($pids, @data);
  $fork->do_fork(\@jobs, \@data);

  if ($fork->result) {
    # all processes(including child of child) are success
    warn "SUCCESS";
  } else {
    # 1 or some child processes(including child of child) are fail
    warn "ERROR";
  }

process tree is like this:

    /usr/bin/perl ./fork.pl
     \_ /usr/bin/perl ./fork.pl
     |   \_ /usr/bin/perl ./fork.pl
     |   \_ /usr/bin/perl ./fork.pl
     \_ /usr/bin/perl ./fork.pl
     \_ /usr/bin/perl ./fork.pl

=head1 DESCRIPTION

=head1 METHODS

=head2 new

 $fork = Fork::ParallelJob->new(%options);

%options takes:

 name        ... name of jobs. if you omit it, use process id.
 max_process  .. max processes. if 0, unlimit. (default: 0).
 retry       ... how many times retry job when job is died (default: 0).
 retry_fork  ... how many times retry fork
 data_format ... data format of parent_data/child_data. Storable(default), YAML, JSON.
 setsid      ... use setsid in child process (default: 0). useful for CGI etc.
 close       ... if true, close STDIN/STDOUT/STDERR (default: 0)
 wait_sleep  ... sleep seconds for waitpid(default: 0.5).

=head2 do_fork

 $fork->do_fork(\@jobs, \@data, $code);

=head3 @jobs

give jobs and data. @jobs is arrayref of code reference or array ref of name => code reference.
for example:

 [
   sub {...},
   sub {...},
   sub {...},
   sub {...},
 ]

or

 [
   name1 => sub {...},
   name2 => sub {...},
   name3 => sub {...},
   name4 => sub {...},
 ]

If you use the later, the name is used in warning message(when job is died or return false).

each job should return 1(succes)/0(fail).

=head3 @data

each data is give to each job.

=head3 $code

$code is executed while waiting for finishing pids.



=head2 result

 $fork->result;

If it returns 1, all processes are success.
If it returns 0, one/some processes fail.

=head2 child

 $child = $fork->child(%options);

It create child of child. parent($fork)'s options are inherited.
if you pass %options, parent's values are overrided.

=head2 add

 $fork->add(sub { ... });
 $fork->add(sub { ... });

add new job.

=head2 parent_data

 $data = $fork->parent_data->get;
 $fork->parent_data->set($data);

It returns Fork::ParallelJob::Data object for parent data.

parent data may be accessed from many children, so you should lock it when reading/writing.

 $pd = $fork->parent_data;
 $pd->lock(
   sub {
     my $data = $pd->get;
     $data->{hoge} = 1;
     $pd->set($data);
   }
 );

=head2 child_data

 $data = $fork->child_data->get;
 $fork->chlid_data->set($data);

It returns Fork::ParallelJob::Data object for child data.

=head1 AUTHOR

Ktat, C<< <ktat at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-fork-paralelljob at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Fork-ParallelJob>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Fork::ParallelJob


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Fork-ParallelJob>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Fork-ParallelJob>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Fork-ParallelJob>

=item * Search CPAN

L<http://search.cpan.org/dist/Fork-ParallelJob/>

=back

=head1 ACKNOWLEDGEMENTS

=head1 LICENSE AND COPYRIGHT

Copyright 2011 Ktat.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1; # Endo of Fork::ParallelJob
