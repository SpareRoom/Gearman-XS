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
use Test::More;
use Storable;
use Gearman::XS qw(:constants);
use FindBin qw( $Bin );
use lib ("$Bin/lib", "$Bin/../lib");
use TestLib;

if ( not $ENV{GEARMAN_LIVE_TEST} ) {
  plan( skip_all => 'Set $ENV{GEARMAN_LIVE_TEST} to run this test' );
}

#plan tests => 10;

my ($ret, $job_handle);
my @handles = ();

my $timeout = 0;

# client
my $client= new Gearman::XS::Client;
isa_ok($client, 'Gearman::XS::Client');
is($client->add_server('127.0.0.1', 4731), GEARMAN_SUCCESS);

# worker
my $worker= new Gearman::XS::Worker;
isa_ok($worker, 'Gearman::XS::Worker');
is($worker->add_server('127.0.0.1', 4731), GEARMAN_SUCCESS);

my $testlib = new TestLib;
$testlib->run_gearmand();
sleep(2);

# gearman server running?
is($client->echo("blubbtest"), GEARMAN_SUCCESS);
is($worker->echo("blahfasel"), GEARMAN_SUCCESS);

my @expected = qw(
    Es6NWiEsTrQlTUTSFIpF9PhVwwkayCPxKS6fKszMU3Bg8Di17Dpxgo
    3DjBezExYbOG
    7OD
    LhldZrDIqlfjjH1p7tTOaSidLqt013wFh72ddeSHUCqWpjvLPJ7WCBYhS
    mOJMiaHN2imnWe
    8n1qh
    tGyM0mE9Zv
    Nr5bzjTVrc1zvtXBP0Ktdt5YWyHEYdDKtgItPYue3XSGenRzRBi
    gGMOqPIfy9ZN1vWvyUUCObTsPi2BXv5r8dg8gQrIAIypb236AJzvMs3DmOy5
    oCtreWC0CjRU4YVSrxSAhAX5z6vR6nDD5zjrCJ1NG7dC3Cy2xPBasw2
);

for my $i (0..$#expected) {
    
    my $rand = $expected[$i];

    is( $worker->add_function("context_$i", 0, \&context, $rand),
        GEARMAN_SUCCESS,
        "function context_$i() added"
    );

    $client->do_background("context_$i", scalar reverse($rand));
}

my $i = 0;
$worker->work for (0..$#expected);

sub context {
  my ($job, $context) = @_;
  is($context, $expected[$i], "context $i is correct");
  is($job->workload, scalar reverse($expected[$i]), "workload $i is correct");
  $i++;
}


done_testing;
