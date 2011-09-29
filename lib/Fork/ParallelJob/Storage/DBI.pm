package Fork::ParallelJob::Storage::DBI;

use strict;
use warnings;
use DBI;
use parent qw(Fork::ParallelJob::Storage);
use Clone;

sub new {
  my $class = shift;
  my $self = bless {
              %{$_[0] || {}}
             }, $class;
  return $self;
}

sub clone {
  my $self = shift;
  my $dbh = delete $self->{dbh};
  my $clone = Clone::clone($self);
  $self->{dbh} = $clone->{dbh} = $dbh;
  $clone;
}

sub _connect {
  my $self = shift;
  $self->{dbh} = DBI->connect(@{$self->{connect_info}}) or die $DBI::errstr;
  $self->{connect_pid} = $$;
  $self->{dbh};
}

sub table {
  $_[0]->{table} || 'job_data';
}

sub cleanup {
  my ($self) = @_;
  $self->dbh->do('delete from ' . $self->table)
}

sub key_name {
  return $_[0]->name;
}

sub dbh {
  my $self = shift;
  if ($self->{connect_pid} and $self->{connect_pid} != $$) {
    $self->_connect();
  }
  $self->_connect if not $self->{dbh} or not $self->{dbh}->ping;
  $self->{dbh}
}

sub lock {
  my ($self, $code) = @_;
  $self->{locked} = 1;
  my $dbh = $self->dbh;
  my $ret = 0;
  eval {
    $dbh->begin_work;
    $code->();
  };
  $self->{locked} = 0;
  my $err = $@;
  eval {
    if ($err) {
      $dbh->rollback;
      warn $err;
      $ret = 0;
    } else {
      $dbh->commit;
      $ret = 1;
    }
  };
  die $@ if $@;
  $self->{selected} = undef;
  return $ret;
}

sub get {
  my $self = shift;
  return $self->_get($self->key_name);
}

sub _get {
  my ($self, $name) = @_;
  my $dbh = $self->dbh;
  my $sth = $dbh->prepare(sprintf('SELECT data, id FROM %s WHERE name = ?', $self->table));
  $sth->execute($name);
  my $data;
  if (my $r = $sth->fetchrow_arrayref) {
    if ($self->{locked}) {
      $sth = $dbh->prepare(sprintf('SELECT data FROM %s WHERE id = ? FOR UPDATE', $self->table));
      $sth->execute($r->[1]);
      $self->{selected} = $r->[1];
    }
    $data = $r->[0];
  }
  return $data;
}

sub set {
  my ($self, $serialized) = @_;
  my $dbh = $self->dbh;
  my $data_sth;
  if (my $id = $self->{selected}) {
    $data_sth = $dbh->prepare(sprintf 'UPDATE %s set data = ? WHERE id = ?', $self->table);
    $data_sth->execute($serialized, $id);
  } else {
    $data_sth = $dbh->prepare(sprintf 'INSERT INTO %s (data, name) values(?, ?)', $self->table);
    $data_sth->execute($serialized, $self->name);
  }
}


sub get_all {
  my ($self) = @_;
  my $dbh = $self->dbh;
  my $sth = $dbh->prepare(sprintf 'SELECT data FROM %s', $self->table);
  $sth->execute;
  my @data;
  while (my $r = $sth->fetchrow_arrayref) {
    push @data, $r->[0];
  }
  return \@data;
}

sub DESTROY {
  my ($self) = @_;
  my $dbh = $self->dbh;
  $dbh->rollback unless $dbh->{AutoCommit};
  $dbh->disconnect;
}

1;

=head1 NAME

Fork::ParallelJob::Storage::DBI -- File storage object for Fork::ParallelJob::Data

=head1 SYNOPSIS

 my $data = Fork::ParallelJob::Storage::DBI->new();
 
 $data->set($data);
 $data->get();

=head1 TABLE DEFINITION

 CREATE TABLE job_data (
   id     integer not null auto_increment,
   name   varchar(255),
   data   blob        ,
   primary key (id)
 ) ENGINE=InnoDB

=head1 METHOD

=head2 new

=head2 name

=head2 prepare

=head2 cleanup

=head2 lock

=head2 get

=head2 _get

=head2 set

=head2 get_all

=head2 files

=head1 AUTHOR

Ktat, C<< <ktat at cpan.org> >>

=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2011 Ktat.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1; # Endo of Fork::ParallelJob::Storage::DBI
