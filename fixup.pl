#!/usr/bin/perl -alpi
use strict;
use warnings;

my ($num) = $ARGV =~ /.*\D(\d+)/ or die "Could not get rows from $ARGV\n";
next if $_ eq "----";
@F == 4 || die "$ARGV line $.: Could not parse '$_'\n";
$F[0] = oct reverse sprintf("%0*bb0", $num, $F[0]);
#$F[3] = 10;
$_ = "@F";


