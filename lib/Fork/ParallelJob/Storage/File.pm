package Fork::ParallelJob::Storage::File;

use strict;
use warnings;
use Fcntl qw/:DEFAULT :flock/;
use File::Path qw/make_path/;
use File::Spec ();
use parent qw(Fork::ParallelJob::Storage);

sub new {
  my $self = shift;
  my %args = (
              base_dir => File::Spec->tmpdir(),
              %{$_[0] || {}}
             );
  return bless \%args, $self;
}

sub prepare {
  my $self = shift;
  unless (-e $self->{base_dir}) {
    make_path $self->{base_dir}
      or die "failed to create directory:$self->{base_dir}:$!";
  }
}

sub cleanup {
  my ($self) = @_;
  for my $fn (@{$self->files}) {
    unlink $fn;
  }
  rmdir $self->{base_dir};
}

sub file_name {
  my $self = shift;
  return "$self->{base_dir}/status_" . $self->name;
}

sub _fh {
  my $self = shift;
  my $fn = shift;
  sysopen my $fh, $fn, O_RDWR | O_CREAT or die "$!: $fn";
  binmode $fh;
  return $fh;
}

sub lock ($&) {
  my ($self, $code) = @_;
  my $fn = $self->file_name;
  local $@;
  eval {
    my $fh = $self->_fh($fn);
    flock $fh, LOCK_EX;
    $self->{fh} = $fh;
    $code->();
    flock $fh, LOCK_UN;
    close $fh;
    delete $self->{fh};
  };
  if ($@) {
    warn $@;
    return 0;
  } else {
    return 1;
  }
}

sub get {
  my $self = shift;
  return $self->_get($self->{fh} || $self->file_name);
}

sub _get {
  my ($self, $fn_or_fh) = @_;
  my $fh = ref $fn_or_fh ? $fn_or_fh : $self->_fh($fn_or_fh);
  binmode($fh);
  seek $fh, 0,0;
  local $/;
  my $data = <$fh>;
  return $data;
}

sub set {
  my ($self, $serialized) = @_;
  my $fh = $self->{fh} || $self->_fh($self->file_name);
  binmode($fh);
  seek $fh ,0, 0;
  truncate $fh, 0;
  print $fh $serialized or die;
}


sub get_all {
  my ($self) = @_;
  my @files = @{$self->files};
  my @data = map $self->_get($_), @files;
  return \@data;
}

sub files {
  my ($self) = @_;
  if (opendir my $dir, $self->{base_dir}) {
    my @files = grep /^status_/, readdir $dir;
    closedir $dir;
    [ map $self->{base_dir} . '/' . $_, @files ];
  } else {
    []
  }
}

1;

=head1 NAME

Fork::ParallelJob::Storage::File -- File storage object for Fork::ParallelJob::Data

=head1 SYNOPSIS

 my $data = Fork::ParallelJob::Storage::File->new();
 
 $data->set($data);
 $data->get();

=head1 DESCRIPTION

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

1; # Endo of Fork::ParallelJob::Storage::File
