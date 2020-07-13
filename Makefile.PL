# Gearman Perl front end
# Copyright (C) 2013 Data Differential, http://datadifferential.com/
# Copyright (C) 2009-2010 Dennis Schoen
# All rights reserved.
#
# This library is free software; you can redistribute it and/or modify
# it under the same terms as Perl itself, either Perl version 5.8.9 or,
# at your option, any later version of Perl 5 you may have available.

use strict;
use warnings;
use ExtUtils::MakeMaker;
use Env qw(GEARMAN_LIB GEARMAN_INCLUDE);
use vars qw($INCLUDES $CCFLAGS);

my $LIBS = '';
$CCFLAGS = '-Wall -Wundef -Wno-shadow -Wmissing-prototypes -fdiagnostics-show-option -Wformat=2 -Wstrict-aliasing -Wextra -Wmissing-declarations -Wcast-align -Wswitch-default -Wswitch-enum -Wno-undef -Wno-unused-value -Wno-unused-variable -Wno-unused-parameter';

if (defined $GEARMAN_LIB) {
  print STDERR "Using GEARMAN_LIB = $GEARMAN_LIB\n";
  $LIBS .= " -L$GEARMAN_LIB ";
}

if (defined $GEARMAN_INCLUDE) {
  print STDERR "Using GEARMAN_INCLUDE = $GEARMAN_INCLUDE\n";
  $INCLUDES .= " -I$GEARMAN_INCLUDE";
}

$LIBS .= ' -lgearman';

if ( $^O eq 'MSWin32' ) {
    print STDERR "Win32 is currently not supported";
    exit(0);
}

my %WriteMakefileArgs = (
  "ABSTRACT" => "Perl front end for the Gearman C library.",
  "AUTHOR" => "Brian Aker <brian\@tangent.org>",
  "VERSION_FROM" => "lib/Gearman/XS.pm",
  "CONFIGURE_REQUIRES" => {
    "ExtUtils::MakeMaker" => "6.52",
  },
  "TEST_REQUIRES" => {
    "Test::More" => "0",
    "Storable" => "0",
  },
  "PREREQ_PM" => {
    "ExtUtils::CBuilder" => "0",
    "Alien::Gearman" => "0",
  },
  "DISTNAME" => "Gearman-XS",
  "LICENSE" => "perl",
  "NAME" => "Gearman::XS",
  META_MERGE => {
     resources => {
       license     => 'http://dev.perl.org/licenses/',
       homepage    => 'https://metacpan.org/release/Gearman-XS',
       bugtracker  => 'https://github.com/SpareRoom/Gearman-XS/issues',
       repository  => 'https://github.com/SpareRoom/Gearman-XS.git',
       IRC         => 'irc://irc.perl.org/#xs'
     },
  },
  XS      => { "XS.xs" => "XS.c", "Worker.xs" => "Worker.c", "Task.xs" => "Task.c", "Job.xs" => "Job.c", "Client.xs" => "Client.c", "Const.xs" => "Const.c" },
  OBJECT => [ "XS\$(OBJ_EXT)", "Worker\$(OBJ_EXT)", "Task\$(OBJ_EXT)", "Job\$(OBJ_EXT)", "Client\$(OBJ_EXT)", "Const\$(OBJ_EXT)" ],
  INC     => $INCLUDES,
  LIBS    => $LIBS,
  CCFLAGS => $CCFLAGS,
  clean   => {
    FILES => q{
      Makefile.old
    },
  },
);

my %FallbackPrereqs = (
    "Alien::Gearman" => "0",
);

unless ( eval { ExtUtils::MakeMaker->VERSION(6.63_03) } ) {
  delete $WriteMakefileArgs{TEST_REQUIRES};
  delete $WriteMakefileArgs{BUILD_REQUIRES};
  $WriteMakefileArgs{PREREQ_PM} = \%FallbackPrereqs;
}

delete $WriteMakefileArgs{CONFIGURE_REQUIRES}
  unless eval { ExtUtils::MakeMaker->VERSION(6.52) };

WriteMakefile(%WriteMakefileArgs);
