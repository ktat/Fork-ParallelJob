 #!/usr/bin/perl

use strict;
use warnings;
use lib qw(../lib/ ./lib);
use Fork::ParallelJob;
use DBI;

my $fork = Fork::ParallelJob->new(jobs_in_data => 0,
                                  no_data => 1,
                                  sleep => 0,
                                  max_process => 20,
                                 );
my $dbh;
$fork->register_jobs({
                      get_file     => sub {
                        my ($f, $url)  = @_;
                        my $c = $f->child;
                        #warn $url;
                        my $path_to_file = $url . '.source';
                        $c->do_fork('extract_file', [$path_to_file]);
                      },
                      extract_file => sub {
                        my ($f, $file) = @_;
                        my $c = $f->child;
                        my $path_to_file = $file . '.extracted';
                        #warn $path_to_file;
                        $c->do_fork('split_file'  , [$path_to_file]);
                      },
                      split_file   => sub {
                        my ($f, $file) = @_;
                        my $c = $f->child;
                        #warn $file;
                        my @splitted_files = map {$file . '.' . $_} (1 .. 5);
                        $c->do_fork('parse_insert', \@splitted_files)
                      },
                      parse_insert => sub {
                        my ($f, $files) = @_;
                        #warn "parse_insert\t", join "\t", @$files;
                        $dbh ||= DBI->connect('dbi:mysql:test;host=dev001.wano.office', 'root','', {RaiseError => 1, PrintError => 0, AutoCommit => 1});
                        for (1 .. 10) {
                          my $sth = $dbh->prepare('insert into aaaa(id) values ' . join ',', ('(0)') x 200);
                          $sth->execute;
                        }
                        warn "inserted";
                      }
                     });
$fork->do_fork('get_file', [1 .. 100]);
$fork->cleanup;
