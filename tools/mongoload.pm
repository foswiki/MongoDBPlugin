#!/usr/bin/perl -w
# See bottom of file for default license and copyright information

use strict;
use warnings;

BEGIN {
    my $goodlibpath = eval {
        require 'setlib.cfg';
        1;
    };
    if ( not $goodlibpath ) {
        die(<<"HERE");
Please start the script with your Foswiki's bin directory in the perl LIB path,
Eg. perl -wT -I /var/lib/foswiki/bin ./mongoload.pm
\tThe error was: '$!'
HERE
    }
    require 'LocalSite.cfg';
}

use Assert;
use File::Find;

ASSERT($Foswiki::cfg{DataDir});
ASSERT($Foswiki::cfg{ScriptDir});

sub wanted {
	my ($thing) = @_;

	if ($_ and -d $_ and not $_ =~ /[\.]/) {
		print "Loading $File::Find::dir...\n";
#		system(<<"HERE");
#cd $Foswiki::cfg{ScriptDir}
#./rest /MongoDBPlugin/update -updateweb $_
#HERE
	}

}

print "Using $Foswiki::cfg{DataDir} for webs...\n";
opendir my($dh), $Foswiki::cfg{DataDir} or die "Couldn't read $Foswiki::cfg{DataDir}: $!";

find({ follow => 1, wanted => \&wanted}, readdir $dh);

1;

__END__
Foswiki - The Free and Open Source Wiki, http://foswiki.org/

Copyright (C) 2008-2011 Foswiki Contributors. Foswiki Contributors
are listed in the AUTHORS file in the root of this distribution.
NOTE: Please extend that file, not this notice.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.
