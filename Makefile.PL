use inc::Module::Install;

name     'Fork-ParalellJob';
all_from 'lib/Fork/ParalellJob.pm';
author   q{Ktat <ktat@cpan.org>};
license  'perl';

build_requires 'Test::More';

auto_install;

WriteAll;

