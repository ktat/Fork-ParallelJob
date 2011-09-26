use inc::Module::Install;

name     'Fork-ParallelJob';
all_from 'lib/Fork/ParallelJob.pm';
author   q{Ktat <ktat@cpan.org>};
license  'perl';

build_requires 'Test::More';

requires
    'File::Path'            => 0,
    'Class::Accessor::Lite' => 0,
    'Class::Load'           => 0,
    'Clone'                 => 0,
    'Data::Dumper'          => 0,
    'Fcntl'                 => 0,
    'JSON::XS'              => 0,
    'POSIX'                 => 0,
    'Storable'              => 0,
    'Time::HiRes'           => 0,
    'YAML::XS'              => 0,
    'parent'                => 0,
    'File::Spec'            => 0,
    ;
auto_install;

WriteAll;

