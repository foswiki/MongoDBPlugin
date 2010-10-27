# See bottom of file for copyright and license details

=begin TML

---+ package Foswiki::Plugins::MongoDBPlugin::HoistMongoDB

extract MonogDB queries from Query Nodes to accellerate querying

=cut

package Foswiki::Plugins::MongoDBPlugin::HoistMongoDB;

use strict;

use Foswiki::Infix::Node ();
use Foswiki::Query::Node ();
use Tie::IxHash ();


use Foswiki::Query::HoistREs ();



use constant MONITOR => 1;

=begin TML

---++ ObjectMethod hoist($query) -> $ref to IxHash


=cut

sub hoist {
    my ($node, $indent) = @_;

    return undef unless ref( $node->{op} );
    
    #use IxHash to keep the hash order - _some_ parts of queries are order sensitive
    my %mongoQuery = ();
    my $ixhQuery            = tie( %mongoQuery, 'Tie::IxHash' );
    #    $ixhQuery->Push( $scope => $elem );
    print STDERR "hoist from: ", $node->stringify(), "\n" if MONITOR;

    return $node->{op}->hoistMongoDB($node);
}


########################################################################################
#Hoist the OP's

=begin TML

---++ ObjectMethod Foswiki::Query::OP_like::hoistMongoDB($node) -> $ref to IxHash

hoist ~ into a mongoDB ixHash query

=cut

package Foswiki::Query::OP_like;
sub hoistMongoDB {
    my $op = shift;
    my $node = shift;
    
    if ( $node->{op}->{name} eq '~' ) {
        
print STDERR "param1(".$node->{params}[0]->{op}."): ", $node->{params}[0]->stringify(), "\n" if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;

        my $lhs = Foswiki::Query::HoistREs::_hoistDOT( $node->{params}[0] );
        my $rhs = Foswiki::Query::HoistREs::_hoistConstant( $node->{params}[1] );
        if ( $lhs && $rhs ) {
            $rhs = quotemeta($rhs);
            $rhs          =~ s/\\\?/./g;
            $rhs          =~ s/\\\*/.*/g;
            $lhs->{regex} =~ s/\000RHS\001/$rhs/g;
            $lhs->{source} = Foswiki::Query::HoistREs::_hoistConstant( $node->{params}[1] );
            return $lhs;
        }
    }
}

=begin TML

---++ ObjectMethod Foswiki::Query::OP_dot::hoistMongoDB($node) -> $ref to IxHash

hoist ~ into a mongoDB ixHash query

=cut

package Foswiki::Query::OP_dot;
our %aliases = (
#    attachments => 'META:FILEATTACHMENT',
#    fields      => 'META:FIELD',
#    form        => 'META:FORM',
#    info        => 'META:TOPICINFO',
#    moved       => 'META:TOPICMOVED',
#    parent      => 'META:TOPICPARENT',
#    preferences => 'META:PREFERENCE',
    name    => '_topic'
);

sub hoistMongoDB {
    my $node = shift;
    
    
}


1;
__DATA__

Module of Foswiki - The Free and Open Source Wiki, http://foswiki.org/, http://Foswiki.org/

Copyright (C) 2010 Foswiki Contributors. All Rights Reserved.
Foswiki Contributors are listed in the AUTHORS file in the root
of this distribution. NOTE: Please extend that file, not this notice.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.

Author: Sven Dowideit http://fosiki.com
