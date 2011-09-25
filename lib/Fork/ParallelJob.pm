package Fork::ParallelJob;

use POSIX ':sys_wait_h', 'setsid';
use Class::Load qw/load_class/;
use Time::HiRes qw/sleep/;
use strict;
use Clone;
use warnings;
use Fork::ParallelJob::Jobs;
use Class::Accessor::Lite
  (
   rw => [qw/nowait retry max_process wait_sleep tmp_name use_data/],
   r  => [qw/name pid count current child_data_format root_data root_data_format is_child pids/],
  );

our $VERSION = '0.01';

sub new {
  my $class = shift;
  my $self = bless
    {
     tmp_name           => '/tmp/fork-paralleljob',
     name               => '',
     close              => undef,
     nowait             => undef,
     setsid             => undef,
     max_process        => 0,
     retry              => 0,
     pid                => $$,
     data_format        => 'Storable',
     root_data_format   => '',
     child_data_format  => '',
     wait_sleep         => 0.5,
     jobs_in_root       => 0,
     use_data           => 1,
     @_,
     pid                => $$,
     pids               => {},
     count              => 0,
     current            => 0,
    }, $class;

  $self->{name} ||= $$;

  if (defined $ENV{SERVER_PROTOCOL} and $ENV{SERVER_PROTOCOL} =~m{^HTTP}i and not $self->{is_child}) {
    $self->{close}  //= 1;
    $self->{nowait} //= 1;
    $self->{setsid} //= 1;
  } else {
    $self->{close}  ||= 0;
    $self->{nowait} ||= 0;
    $self->{setsid} ||= 0;
  }

  if ($self->{max_process} and $self->{nowait}) {
    Carp::carp("cannot use max_process with nowait");
  }
  if ($self->{jobs_in_root} and not $self->{use_data}) {
    Carp::carp("cannot use jobs_in_root without use_data");
  }

  if ($self->use_data) {
    for my $key ('root', 'child') {
      next if $self->{$key . '_data'};

      my $data_class = 'Fork::ParallelJob::Data::' . ($self->{$key . '_data_format'} || $self->{data_format});
      load_class($data_class);
      $self->{$key . '_data'}  ||= $data_class->new(base_dir => $self->tmp_name . "-$key-"  . $self->{'name'});
    }
    $self->{parent_data} ||= $self->root_data;
  }

  unless ($self->{jobs}) {
    if (! $self->{is_child} and $self->{jobs_in_root} ) {
      Carp::croak(ref($self->{'root_data'}) . " cannot store job.") unless $self->{'root_data'}->can_job_store;
      load_class('Fork::ParallelJob::Jobs::RootData');
      $self->{jobs} = Fork::ParallelJob::Jobs::RootData->new($self->{'root_data'});
      $self->{root_jobs} ||= $self->{jobs};
    } else {
      $self->{jobs} = Fork::ParallelJob::Jobs->new;
    }
    if (! $self->{is_child} and $self->use_data) {
      $self->root_data->set_worker_id($self->{'name'});
    }
  }
  return $self;
}

sub child {
  my ($self) = shift;
  my %opt = @_;
  my $clone = Clone::clone {%$self};
  if ($self->{jobs_in_root}) {
    $clone->{root_jobs} ||= $clone->{jobs};
  }

  # not inherit these data
  undef $clone->{$_} for qw/jobs setsid nowait close/;

  Carp::carp("'name' option is autmatically defined. ignored.") if exists $opt{name};
  my $o = (ref $self)->new(%$clone, %opt, is_child => 1);
  if ($self->{use_data}) {
    $o->{parent_data} = $clone->{child_data};
    $o->child_data->worker_id($$);
  }
  $o->{name} = $o->{name} . '-child-' . $$;
  return $o;
}

sub add_root_job {
  my ($self, $jobs, $data) = @_;
  if (ref $jobs eq 'ARRAY') {
    $self->{root_jobs}->add_multi($jobs, $data);
  } else {
    $self->{root_jobs}->add($jobs, $data);
  }
}

sub add_job {
  my ($self, $jobs, $data) = @_;
  if (ref $jobs eq 'ARRAY') {
    $self->{jobs}->add_multi($jobs, $data);
  } else {
    $self->{jobs}->add($jobs, $data);
  }
}

sub take_job {
  my ($self) = @_;
  $self->{jobs}->take;
}

sub has_job {
  my ($self) = @_;
  $self->{jobs}->num_of_jobs;
}

sub do_fork {
  my $self = shift;
  my $jobs = $self->{jobs} ||= [];
  my $data = $self->{data} ||= [];
  if (@_) {
    my $jobs = shift;
    my $data;
    @$data= @{shift() || []} if defined $_[0];
   if (ref $jobs eq 'CODE') {
      $jobs = ref $data eq 'ARRAY' ? [($jobs) x @{$data}] : [$jobs];
    } elsif (ref $jobs ne 'ARRAY') {
      Carp::croak("first argument must be ARRAY reference of code reference or 1 code reference: $jobs");
    }
    if (defined $data) {
      if (ref $data ne 'ARRAY') {
        Carp::croak("data(second argument) must be ARRAY reference or don't give any data.");
      } elsif (ref $jobs eq 'ARRAY' and @{$data} != @{$jobs}) {
        Carp::croak("num of code and num of data must be same or  don't give any data.");
      }
    }
    while (my $job = shift @$jobs) {
      my $d = shift @$data;
      $self->add_job(ref $job eq 'CODE' ? ($job, $d) : $job, $d);
    }
  }
  $self->{count}++;
  $self->{current}++;

  my $pids = $self->{pids};
  my ($job_name_job, $job_data) = $self->take_job;
  my ($job_name, $job) = %{$job_name_job || {}};
  my $retry_fork = $self->{retry_fork};
  $self->{parent} = $self->{name};

  local $SIG{CHLD} = sub {};
  $SIG{CHLD} = 'IGNORE'  if $self->{setsid};
 FORK:
  if (my $pid = fork()) {
    # parent
    $pids->{$pid} = $job_name;
    if ($self->{max_process} and (scalar keys %$pids) >= $self->{max_process}) {
      # use blocking wait to wait finishing any one process.
      $self->_wait_one_pid;
    }
    if ($self->has_job) {
      $self->do_fork;
    } elsif (! $self->{nowait}) {
      $self->wait_all_children;
    }
  } elsif (defined $pid) {
    # child
    $self->{in_child} = 1;
    POSIX::setsid() if $self->{setsid};
    $self->{unique_name} = ($self->{parent} ? $self->{parent} . '-' . $$ . '-' . ($job_name || $self->{count}) : $$ . '-' . ($job_name || $self->{count}));
    if ($self->use_data) {
      $self->child_data->set_worker_id($self->{unique_name});
    }
    delete $self->{parent};
    my $retry = $self->{retry};
    my $status;
  EXEC: {
      if ($self->{setsid}) {
        setsid or die "Can't start a new session: $!";
      }
      if ($self->{close}) {
        close STDIN;
        close STDOUT;
        close STDERR;
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
      if ($self->use_data) {
        $self->child_data->lock_store(sub {my $data = shift;
                                           $data->{_}{result} = $status ? 1 : 0;
                                           $data->{_}{pid} = $$;
                                           $data->{_}{name} = $job_name;
                                           $data}
                                     );
      }
    }
    CORE::exit(($status ? 0 : 1) & $self->result);
    # CORE::exit($status ? 0 : 1);
  } else {
    # error
    redo FORK if $retry_fork-- > 0;

    die "cannot fork $job_name";
  }
}

sub wait_all_children {
  my ($self) = @_;
  if ($self->{close}) {
    close STDIN;
    close STDOUT;
    close STDERR;
  }
  while (keys %{$self->{pids}}) {
    my $kid = $self->_wait_one_pid(WNOHANG);
    last if $kid == -1;
  }
  if ($self->{is_child} and $self->use_data) {
    $self->parent_data->lock_store(sub {my $d = shift; $d->{_}{result} //= 1; $d->{_}{result} &= $self->result; $d });
  } else {
    $self->{wait_all_children_done} = 1;
  }
}

sub DESTROY {
  my ($self) = @_;
  if (not $self->{is_child} and not $self->{in_child} and not $self->{wait_all_children_done}) {
    $self->wait_all_children;
  }
}

sub _wait_pids {
  my ($self, $flg) = @_;
  my $pids = $self->{pids};
  my $kid;
}

sub _wait_one_pid {
  my $self = shift;
  my $flg = shift;
  my $pids = $self->{pids};
  my $kill_pid;
  while (1) {
    $kill_pid = waitpid -1, $flg ||= 0;
    redo if ! exists $pids->{$kill_pid};
    # process running or no child processes
    last if $kill_pid == 0 || $kill_pid == -1;

    # pid is killed
    my $job_name = delete $pids->{$kill_pid};
    $self->{current}--;
    if ($? == -1) {
      $self->{result}->{$kill_pid} = undef;
    } else {
      my $exit = $?;
      $self->{result}->{$kill_pid} ||=
        {
         name     => $job_name,
         $exit & 127 ?
         (
          signal   => ($exit & 127),
          coredump => ($exit & 128 ? 1 : 0),
          exit     => ($exit >> 8),
         ) : (exit => ($exit >> 8), origin => $exit)
        };
    }
    last;
  }
  sleep $self->{wait_sleep} if %$pids;
  return $kill_pid;
}

sub result {
  my $self = shift;
  $self->use_data ? $self->result_from_data : $self->result_from_exit;
}

sub result_from_exit {
  my $self = shift;
  my $result = 0;
  foreach my $pid (keys %{$self->{result}}) {
    $result |= $self->{result}->{$pid}->{exit};
  }
  return $result ? 0 : 1;
}

sub result_from_data {
  my ($self) = @_;
  my $result = 1;
  foreach my $data (@{$self->child_data->get_all}) {
    next if not exists $data->{_}{result};
    ($result &= $data->{_}{result}) or last;
  }
  return $result;
}

sub child_data {
  my $self = shift;
  $self->{child_data}
}

sub parent_data {
  my $self = shift;
  $self->{parent_data}
}

sub root_data {
  my $self = shift;
  $self->{root_data}
}

sub cleanup {
  my $self = shift;
  if ($self->use_data) {
    $self->root_data->cleanup;
    $self->child_data->cleanup;
  }
}

1;

=head1 NAME

Fork::ParallelJob -- simply do jobs parallelly using fork

=head1 SYNOPSIS

do same job with different data parallelly.

  my $fork = Fork::ParallelJob->new(max_process => 3, name => "fork1");
  my $job = sub {
    my $f = shift; # $fork object
    my $data = shift;
  };
  $fork->do_fork($job, \@data);
  # waiting all job are finished
  $fork->wait_all_children;

  print $fork->result ? 'all success' : 'one/some fail';

do different jobs parallelly.

  my $now = time;
  my $fork = Fork::ParallelJob->new(max_process => 3, name => "fork1");
  # the following jobs will do with fork
  my @jobs = (
              sub
              {
                # child of child
                my $child = $fork->child(name => "fork2", max_process => 2, retry => 4);
                $child>do_fork([
                                 name1 => sub { print "1-1c - $$\n";sleep 4;  print 1,"\t -- \t",time - $now,"\n" },
                                 name2 => sub { print "1-2c - $$\n";sleep 5;  print 2,"\t -- \t",time - $now,"\n" },
                               ]);
              }
              sub { print 2,"p - $$\n"; sleep 1;  print 1,"\t",time - $now,"\n" },
              sub { print 3,"p - $$\n"; sleep 2;  print 2,"\t",time - $now,"\n" },
              sub { print 4,"p - $$\n"; sleep 3;  print 3,"\t",time - $now,"\n" },
             );

  my (@data) = ();
  $fork->do_fork(\@jobs, \@data);
  print $fork->result ? 'all success' : 'one/some fail';

process tree is like this:

    /usr/bin/perl ./fork.pl
     \_ /usr/bin/perl ./fork.pl
     |   \_ /usr/bin/perl ./fork.pl
     |   \_ /usr/bin/perl ./fork.pl
     \_ /usr/bin/perl ./fork.pl
     \_ /usr/bin/perl ./fork.pl

You can check process tree by the command like the following on Linux.

 % watch 'ps -ef f | grep fork.pl'

=head1 DESCRIPTION

This module can simply do job(code ref) parallelly.

=head1 METHODS

=head2 new

 $fork = Fork::ParallelJob->new(%options);

%options takes:

=over 4

=item  name

name of jobs. if you omit it, use process id.

=item  max_process

max processes. if 0, unlimit. (default: 0).
You cannot use this option with nowait/setsid option.

=item  retry

how many times retry job when job is died (default: 0).

=item  retry_fork

how many times retry fork

=item  data_format

data format of root_data/child_data. Storable(default), YAML, JSON.

=item  root_data_format

data format for root data. if omitted, use data_format.

=item  child_data_format

data format for child data. if omitted, use data_format.

=item  setsid

use setsid in child process. default value is 0.
NOTE THNT: not child and SERVER_PROTOCOL environment value is /^HTTP/, default value is 1.

=item  close

if true, close STDIN/STDOUT/STDERR).  default value is 0.
NOTE THNT: not child and SERVER_PROTOCOL environment value is /^HTTP/, default value is 1.

=item nowait

not wait children. If you use this option, use wait_all_children method to wait children.
defualt value is 0.
NOTE THNT: not child and SERVER_PROTOCOL environment value is /^HTTP/, default value is 1.

=item  wait_sleep

sleep seconds for waitpid(default: 0.5).

=item  jobs_in_root

jobs are saved into root data(use Fork::ParallelJob::Jobs::RootData instead of Fork::ParallelJob::Jobs).
L</"about jobs_in_root new parameter">

You cannot set use_data 0 when jobs_in_root is true.

=item tmp_name

temorary directory name prefix. default is.

 '/tmp/fork-paralleljob'

Actual directories are:

 /tmp/fork-paralleljob-child-$name
 /tmp/fork-paralleljob-root-$name

=back

=head2 do_fork

 $fork->do_fork(\@jobs, \@data, $code);

fork given jobs.

=over 4

=item @jobs

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
$fork object is given to each code ref.

=item @data

each data is give to each job.

=item $code

$code is executed while waiting for finishing pids.
$fork object is given to $code.

=back

=head2 result

 $fork->result;

If it returns 1, all processes are success. If it returns 0, one/some processes fail.

If use_data is true, it uses $fork->result_from_data.
If use_data is false, it uses $fork->result_from_exit.

=head2 result_from_exit

 $fork->result_from_exit;

return result from exit code(success is 0).

=head2 result_from_data

 $fork->result_from_data;

return result from data (if code returns 1, success)

=head2 use_data

 $fork->use_data

If use child/parent data, it returns 1. default is 1.

=head2 child

 $child = $fork->child(%options);

It create child of child. parent($fork)'s options except nowait, setsid and close are inherited.
if you pass %options, parent's values are overrided.

You cannot call this method when jobs_in_root is true.

=head2 add_job

 $fork->add_job(sub { ... }, $data);
 $fork->add_job([sub { ... }, ...], [$data, ...]);
 $fork->add_job({job_name => sub { ... }}, $data);
 $fork->add_job([{job_name => sub { ... }}, ...], [$data, ...]);
 $fork->add_job($job_name, $data);
 $fork->add_job([$job_name, ...], [$data, ..]);

add new job.

=head2 add_root_job

 $fork->add_root_job(sub { ... }, $data);
 $fork->add_root_job([sub { ... }, ...], [$data, ...]);
 $fork->add_root_job({job_name => sub { ... }}, $data);
 $fork->add_root_job([{job_name => sub { ... }}, ...], [$data, ...]);
 $fork->add_root_job($job_name, $data);
 $fork->add_root_job([$job_name, ...], [$data, ..]);

add new job to root process.
To use this emthod, jobs_in_root must be true.

=head2 take_job

 $fork->take_job;

get job and remove from jobs.

=head2 has_job

 $fork->has_job

return true if jobs is not empty.

=head2 root_data

 $data = $fork->root_data->get;
 $fork->root_data->set($data);

It returns Fork::ParallelJob::Data object for root data.

root data may be accessed from many children, so you should lock it when reading/writing.

 $pd = $fork->parent_data;
 $pd->lock_store(
   sub {
     my $data = shift # return value of $pd->get;
     $data->{hoge} = 1;
     return $data;    # $pd->set($data) is called;
   }
 );

=head2 parent_data

This method usage is as same as root_data.

 root
  +- child (root_data is equal to parent_data)
     +- grandchild (root_data is root's data, parent_data is child's data)

=head2  wait_all_children

If you use nowait option for new, use this method to wait all children.
If you don't call this, automatically called in do_fork.

=head2 child_data

 $data = $fork->child_data->get;
 $fork->chlid_data->set($data);

It returns Fork::ParallelJob::Data object for child data.

=head2 cleanup

 $fork->cleanup;

delete root_data and child_data.
This is shortcut:

 $fork->root_data->cleanup;
 $fork->child_data->cleanup;

=head1 about jobs_in_root new parameter

If you set jobs_in_root as true, you can add jobs from child process.

  my $fork = Fork::ParallelJob->new(max_process => 3, name => "fork1", close => 0, setsid => 0);
  # the following jobs will do with fork
  $fork->add_job(sub { print "this is child process". my $f = shift; $f->add_job(sub { print 'inserted from child'})})
  $fork->add_job(sub { print "this is child process". my $f = shift; $f->add_job(sub { print 'inserted from child'})})
  $fork->do_fork;

If you want to control total max processes and you want to add jobs from child process, this option is useful.

This mode cannot use variables which defined in out of code ref.
In the following case, $time used in code ref is no use.

  my $now = time;
  my $fork = Fork::ParallelJob->new(max_process => 3, name => "fork1", close => 0, setsid => 0);
  # the following jobs will do with fork
  $fork->add_job(sub { print $time. my $f = shift; $f->add_job(sub { print 'inserted from child'})})
  $fork->do_fork;

=head1 useing in CGI etc.

If you use this module for serverside program. the following options are set.

 setsid ... 1
 close  ... 1

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
