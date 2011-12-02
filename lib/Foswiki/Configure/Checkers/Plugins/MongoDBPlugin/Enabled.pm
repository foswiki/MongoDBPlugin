# See bottom of file for license and copyright information
package Foswiki::Configure::Checkers::Plugins::MongoDBPlugin::Enabled;

use strict;
use warnings;

use Foswiki::Configure::Checker ();
use File::Spec();
our @ISA = qw( Foswiki::Configure::Checker );

sub check {
    my $this = shift;
    my $e    = '';

    if ( $Foswiki::cfg{Plugins}{MongoDBPlugin}{Enabled} ) {
        if (
            not $Foswiki::cfg{Store}{Listeners}
            {'Foswiki::Plugins::MongoDBPlugin::Listener'} )
        {
            $e .= $this->WARN(<<'MESSAGE');
You need
<code>{Store}{Listeners}</code> (expert setting) to contain
<code>'Foswiki::Plugins::MongoDBPlugin::Listener' => 1</code>
 so that MongoDBPlugin can keep its data synchronised with Foswiki's Store. For
example: <pre>{
    'Foswiki::Plugins::MongoDBPlugin::Listener' => 1
}</pre>
MESSAGE
        }
        if ( $Foswiki::cfg{Store}{QueryAlgorithm} ne
            'Foswiki::Store::QueryAlgorithms::MongoDB' )
        {
            $e .= $this->WARN(<<'MESSAGE');
You need
<code>{Store}{QueryAlgorithm}</code> to be set to
<code>Foswiki::Store::QueryAlgorithms::MongoDB</code>
 so that MongoDBPlugin will be used to handle searches and queries
MESSAGE
        }
        if ( $Foswiki::cfg{Store}{SearchAlgorithm} ne
            'Foswiki::Store::SearchAlgorithms::MongoDB' )
        {
            $e .= $this->WARN(<<'MESSAGE');
You need
<code>{Store}{SearchAlgorithm}</code> to be set to
<code>Foswiki::Store::SearchAlgorithms::MongoDB</code>
 so that MongoDBPlugin will be used to handle searches and queries
MESSAGE
        }
    }
    else {
        if ( $Foswiki::cfg{Store}{Listeners}
            {'Foswiki::Plugins::MongoDBPlugin::Listener'} )
        {
            $e .= $this->ERROR(<<'MESSAGE');
<code>{Store}{Listeners}</code> is using
<code>Foswiki::Plugins::MongoDBPlugin::Listener</code>
 but MongoDBPlugin is disabled. It should perhaps contain no listeners, which
would mean a value of <code>{}</code> or, if you have other listeners
configured apart from MongoDBPlugin, just remove the
 <code>'Foswiki::Plugins::MongoDBPlugin::Listener' => 1</code> part
MESSAGE
        }
        if ( $Foswiki::cfg{Store}{QueryAlgorithm} eq
            'Foswiki::Store::QueryAlgorithms::MongoDB' )
        {
            $e .= $this->ERROR(<<'MESSAGE');
<code>{Store}{QueryAlgorithm}</code> is set to
<code>Foswiki::Store::QueryAlgorithms::MongoDB</code> but MongoDBPlugin is disabled
MESSAGE
        }
        if ( $Foswiki::cfg{Store}{SearchAlgorithm} eq
            'Foswiki::Store::SearchAlgorithms::MongoDB' )
        {
            $e .= $this->ERROR(<<'MESSAGE');
<code>{Store}{SearchAlgorithm}</code> is set to
<code>Foswiki::Store::SearchAlgorithms::MongoDB</code> but MongoDBPlugin is disabled
MESSAGE
        }
    }

    return $e;
}

1;
__END__
Foswiki - The Free and Open Source Wiki, http://foswiki.org/

Copyright (C) 2008-2011 Foswiki Contributors. Foswiki Contributors
are listed in the AUTHORS file in the root of this distribution.
NOTE: Please extend that file, not this notice.

Additional copyrights apply to some or all of the code in this
file as follows:

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.
