# See bottom of file for copyright and license details

=begin TML

---+ package Foswiki::Plugins::MongoDBPlugin::HoistMongoDB

extract MonogDB queries from Query Nodes to accellerate querying

=cut

package Foswiki::Plugins::MongoDBPlugin::HoistMongoDB;

use strict;
use warnings;

use Foswiki::Infix::Node ();
use Foswiki::Query::Node ();
use Tie::IxHash          ();
use Data::Dumper;
use Error::Simple;
use Assert;

use Foswiki::Query::HoistREs ();

use constant MONITOR => 0;
use constant WATCH   => 0;

=begin TML

---++ ObjectMethod hoist($query) -> $ref to IxHash


=cut

sub hoist {
    my ( $node, $indent ) = @_;

    return undef unless ref( $node->{op} );

    print STDERR "hoist from: ", $node->stringify(), "\n" if MONITOR or WATCH;

#TODO: use IxHash to keep the hash order - _some_ parts of queries are order sensitive
#    my %mongoQuery = ();
#    my $ixhQuery = tie( %mongoQuery, 'Tie::IxHash' );
#    $ixhQuery->Push( $scope => $elem );
    my $mongoDBQuery;

    $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::_hoist($node);

#TODO: sadly, the exception throwing wasn't working so I'm using a brutish propogate error
    if ( defined( $mongoDBQuery->{ERROR} ) ) {
        print STDERR "AAAAARGH " . $mongoDBQuery->{ERROR} . "\n";
        return;
    }

    print STDERR "Hoisted to:  ",    #$node->stringify(), " -> /",
      Dumper($mongoDBQuery), "/\n"
      if MONITOR or WATCH;

    if ( defined( $Foswiki::cfg{Plugins}{MongoDBPlugin}{UseJavascriptQuery} )
        and $Foswiki::cfg{Plugins}{MongoDBPlugin}{UseJavascriptQuery} )
    {
        #convert the entire query to a javascript $where clause for testing
        return {'$where' => convertToJavascript($mongoDBQuery)};
    } else {
#need to test to see if we need to re-write parts of the query in javascript
#ie, nested OR's
#one reason for doing it here, after we've made the mongo queries, is that later, they may implement it and we can remove the kludge
        kludge($mongoDBQuery);
    }

    return $mongoDBQuery;
}

sub kludge {
    my $node = shift;
    my $inOr = shift;
    
    foreach my $key (keys(%$node)) {
        my $value = $node->{$key};
        my $thisIsOr = ($key eq '$or');
        if ($inOr and $thisIsOr) {
            #nested OR detected, replace with $where and hope
            delete $node->{$key};
            $node->{'$where'} = convertToJavascript({$key => $value});
        } else {
            if (ref($value) eq 'HASH') {
                kludge($value, ($inOr or $thisIsOr));
            } elsif (ref($value) eq 'ARRAY') {
                next if ($key eq '$in');
                next if ($key eq '$nin');
                foreach my $n (@$value) {
                    kludge($n, ($inOr or $thisIsOr));
                }
            } else {
            }
        }
    }
}


sub _hoist {
    my $node = shift;

    die 'node eq undef' unless defined($node);

    #name, or constants.
    if ( !ref( $node->{op} ) ) {
        return Foswiki::Query::OP_dot::hoistMongoDB( $node->{op}, $node );
    }
    if ( ref( $node->{op} ) eq 'Foswiki::Query::OP_dot' ) {
        return Foswiki::Query::OP_dot::hoistMongoDB( $node->{op}, $node );
    }
    print STDERR "???????" . ref( $node->{op} ) . "\n" if MONITOR or WATCH;

    #TODO: if 2 constants(NUMBER,STRING) ASSERT
    #TODO: if the first is a constant, swap

    $node->{lhs} = _hoist( $node->{params}[0] )
      if ( $node->{op}->{arity} > 0 );
    $node->{ERROR} = $node->{lhs}->{ERROR}
      if ( ref( $node->{lhs} ) and defined( $node->{lhs}->{ERROR} ) );

    $node->{rhs} = _hoist( $node->{params}[1] )
      if ( $node->{op}->{arity} > 1 );
    $node->{ERROR} = $node->{rhs}->{ERROR}
      if ( ref( $node->{rhs} ) and defined( $node->{rhs}->{ERROR} ) );

    monitor($node);

#DAMMIT, I presume we have oddly nested eval/try catch so throwing isn't working
#throw Error::Simple( 'failed to Hoist ' . ref( $node->{op} ) . "\n" )
# unless ( $node->{op}->can('hoistMongoDB') );
    unless ( $node->{op}->can('hoistMongoDB') ) {
        $node->{ERROR} = 'can\'t Hoist ' . ref( $node->{op} );
    }
    if ( defined( $node->{ERROR} ) ) {
        print STDERR "HOIST ERROR: " . $node->{ERROR};
        return $node;
    }

    return $node->{op}->hoistMongoDB($node);
}

sub monitor {
    my $node = shift;

    print STDERR "\nparam0(" . $node->{params}[0]->{op} . "): ",
      Data::Dumper::Dumper( $node->{params}[0] ), "\n"
      if MONITOR;
    print STDERR "\nparam1(" . $node->{params}[1]->{op} . "): ",
      Data::Dumper::Dumper( $node->{params}[1] ), "\n"
      if MONITOR;

    #TODO: mmm, do we only have unary and binary ops?

    print STDERR "----lhs: "
      . Data::Dumper::Dumper( $node->{lhs} )
      . "----rhs: "
      . Data::Dumper::Dumper( $node->{rhs} ) . " \n"
      if MONITOR;



#print STDERR "HoistS ",$query->stringify()," -> /",Dumper($mongoDBQuery),"/\n";

    print STDERR "Hoist node->op="
      . Dumper( $node->{op} )
      . " ref(node->op)="
      . ref( $node->{op} ) . "\n"
      if MONITOR;
}

#map mongodb $ops to javascript logic
my %js_op_map = (
    '$eq'  => '==',    #doesn't exist, probly should for simplicity
    '$ne'  => '!=',
    '$not' => '!',
    '$lt'  => '<',
    '$lte' => '<=',
    '$gt'  => '>',
    '$gte' => '>=',
    '$and' => '&&',
    '$or'  => '||',

    #'' => '',
);

#converts a mongodb query hash into a $where clause
#frustratingly, both (field:value) and {field:{$op,value}} and {$op, {nodes}} and {$op, []} are valid, making it a little messy
#and {$op, []} might refer to the key outside it (as in $nin), or not, as in $or
sub convertToJavascript {
    my $node      = shift;
    my $statement = '';
    
#print STDERR "\n..............DUMPER: ".Dumper($node)."\n";
    
    while ( my ( $key, $value ) = each(%$node) ) {
        $statement .= ' && ' if ( $statement ne '' );

        #convert $ops into js ones
        my $js_key = $key;
        $js_key = $js_op_map{$key} if ( defined( $js_op_map{$key} ) );

        if ( ref($value) eq 'HASH' ) {
            my ($k, $v) = each(%$value);
            if (($k eq '$in') or ($k eq '$nin')) {
                #TODO: look up to see if javascrip thas an value.in(list) or ARRAY.contains(value)
                $statement .= join(' || ', map {
                                    "$js_key == $_"
                                } @$v);
                $statement = (($key eq '$nin')?'!':'')." ($statement) ";
            } else {
                $value = convertToJavascript($value);

                $statement .= "$js_key $value";
            }
        }
        elsif ( ref($value) eq 'ARRAY' ) {
            if (($key eq '$in') or ($key eq '$nin')) {
                die 'unpleasently'; #should never get here - it needs to be handled while we knoe the field it refers to
            } elsif ($key eq '$or') {
                #er, assuming $key == $or - $in and $nin will kick me
                $statement .=
                  join( ' ' . $js_key . ' ', map { convertToJavascript($_) } @$value );

                #$statement = " ($statement) ";
            } else {
                die 'sadly '.$key;
            }


        }
        else {
            if ( $key eq '$where' ) {
                $statement = " ($value) ";
            }
            else {

                #value isa string..
                #TODO: argh, string or number, er, or regex?
#print STDERR "convertToJavascript - $value is a ".ref($value)."\n";
                if (ref($value) eq 'Regexp') {
                    $value =~ /\(\?-xism:(.*)\)/; #TODO: er, regex options?
                    $statement .= "( /$1/.test(this.$js_key) )";
                } else {
                    $statement .= "this.$js_key == '$value'";
                }
            }
        }
    }
    return $statement;
}

########################################################################################
#Hoist the OP's

=begin TML

---++ ObjectMethod Foswiki::Query::OP_eq::hoistMongoDB($node) -> $ref to IxHash

hoist ~ into a mongoDB ixHash query

=cut

package Foswiki::Query::OP_eq;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    ASSERT( $node->{op}->{name} eq '=' ) if DEBUG;
    return { $node->{lhs} => $node->{rhs} };
}

=begin TML

---++ ObjectMethod Foswiki::Query::OP_like::hoistMongoDB($node) -> $ref to IxHash

hoist ~ into a mongoDB ixHash query

=cut

package Foswiki::Query::OP_like;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    my $rhs = quotemeta( $node->{rhs} );
    $rhs =~ s/\\\?/./g;
    $rhs =~ s/\\\*/.*/g;
    $rhs = qr/$rhs/;

    return { $node->{lhs} => $rhs };
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
    name => '_topic',
    web  => '_web',
    text => '_text'
);

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    if ( !defined( $node->{op} ) ) {
        print STDERR 'CONFUSED: ' . Data::Dumper::Dumper($node) . "\n";
        die 'here';

        #return;
    }

    if ( ref( $node->{op} ) ) {

        #an actual OP_dot
        my $lhs = $node->{params}[0];
        my $rhs = $node->{params}[1];

        #        ASSERT( !ref( $lhs->{op} ) ) if DEBUG;
        #        ASSERT( !ref( $rhs->{op} ) ) if DEBUG;
        #        ASSERT( $lhs->{op} eq Foswiki::Infix::Node::NAME ) if DEBUG;
        #        ASSERT( $rhs->{op} eq Foswiki::Infix::Node::NAME )
        #          if DEBUG;

        $lhs = $lhs->{params}[0];
        $rhs = $rhs->{params}[0];
        if ( $Foswiki::Query::Node::aliases{$lhs} ) {
            $lhs = $Foswiki::Query::Node::aliases{$lhs};
        }

        #        print STDERR "hoist OP_dot("
        #          . ref( $node->{op} ) . ", "
        #          . Data::Dumper::Dumper($node)
        #          . ")\n INTO "
        #          . $lhs . '.'
        #          . $rhs . "\n";

        if ( $lhs =~ s/^META:// ) {
            return $lhs . '.' . $rhs;
        }
        else {

            # TODO: assumes the term before the dot is the form name??? gads
            return 'FIELD.' . $rhs . '.value';
        }
    }
    elsif ( $node->{op} == Foswiki::Infix::Node::NAME ) {

        #        print STDERR "hoist OP_dot("
        #          . $node->{op} . ", "
        #          . $node->{params}[0] . ")\n";

        #TODO: map to the MongoDB field names (name, web, text, fieldname)
        return $aliases{ $node->{params}[0] }
          if ( defined( $aliases{ $node->{params}[0] } ) );
        return 'FIELD.' . $node->{params}[0] . '.value';
    }
    elsif (( $node->{op} == Foswiki::Infix::Node::NUMBER )
        or ( $node->{op} == Foswiki::Infix::Node::STRING ) )
    {
        return $node->{params}[0];
    }
}

package Foswiki::Query::OP_and;
use Foswiki::Plugins::MongoDBPlugin::HoistMongoDB;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    #monogdb queries all have only one hash key
    my ( $key, $val ) = each( %{ $node->{lhs} } );

    #print STDERR "---- $key, $val\n";
    if ( defined($key)
        and ( defined( $node->{rhs}->{$key} ) ) )
    {
        my $rhsval = $node->{rhs}->{$key};
        print STDERR "---- $key, $val, $rhsval, ("
          . ref($val) . ")("
          . ref($rhsval) . ")\n";

        if ( not( $key =~ /^\$/ ) ) {

            #anding non-operators
            print STDERR "---------- ref($val) == " . ref($val) . "\n";
            if (    ( not ref($val) )
                and ( not ref($rhsval) )
                and ( $val eq $rhsval ) )
            {

                #A and A == A
                print STDERR "SIIIIIIIIIIIIIIIIIIIIIIIIIIMPLIFY\n";
                return $node->{lhs};
            }
            elsif ( ( ref($val) eq 'HASH' ) and ( ref($rhsval) eq 'HASH' ) ) {

                #maybe we know how to combine these 2 ops
                my ( $ikey, $ival ) = each( %{$val} );
                print STDERR "----i $ikey, $ival - "
                  . join( ',', each( %{$rhsval} ) ) . "\n";
                if ( defined($ikey)
                    and ( defined( $rhsval->{$ikey} ) ) )
                {
                    my $irhsval = $rhsval->{$ikey};
                    print STDERR
                      "SIIIIIIMCOMPLEXIFY: $key : $ikey [$ival, $irhsval]\n";
                    if ( $ikey eq '$ne' ) {
                        return { $key => { '$nin' => [ $ival, $irhsval ] } };
                    }
                }
            }
        }
        else {

            #$key == op, so it should be $or, $in, $nin etc
            #if ( ( ref($val) eq 'ARRAY' ) and ( ref($rhsval) eq 'ARRAY' ) ) {
            if ( $key eq '$or' ) {

               #(a OR b) AND (c OR d OR e)
               #identify the least complex, and leave the more complex one alone
               #for now, doe the first, leave the second

    #return {'$or' =>$rhsval, '$where' => convertToJavascript({'$or' => $val})};
            }
        }

        print STDERR
          "MongoDB cannot AND 2 queries with the same key ($key, $val) \n";

        # re-write one as $where
        return {
            $key => $rhsval,
            '$where' =>
              Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                { $key => $val }
              )
        };

        #die "MongoDB cannot AND 2 queries with the same key ($key, $val) \n";
    }

    return { %{ $node->{lhs} }, %{ $node->{rhs} } };
}

#TODO: mmmm, this isn't going to work.
#mongodb doesn't have the expresiveness to convert competently
#and the only reason i'm doing it is to cope with its inablilty to nest/AND OR's
#i'm told that internally, it can actually deal with multiple keys of the same name, so it might make more sense to implement at
#for now, when we hit a complexity, we can still toss off to a javascript $where clause
sub convertORintoAND {
    my $ORedArray = shift;

    #convert (a OR b) into !(!a AND !b)
    my $query = {};
    foreach my $elem (@$ORedArray) {
        my ( $k, $v ) = each(%$elem);
        if ( defined( $query->{$k} ) ) {
            if ( defined( $query->{$k}->{'$not'} ) ) {

                #convert to $nin
                $query->{$k}->{'$nin'} = [ $query->{$k}->{'$not'}, $v ];
                delete $query->{$k}->{'$not'};
            }
            else {

                #push into $nin
                push( @{ $query->{$k}->{'$nin'} }, $v );
            }
        }
        else {
            $query->{$k} = { '$not' => $v };
        }
    }
    return $query;
}

package Foswiki::Query::OP_or;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    #need to detect nested OR's and unwind them
    if ( defined( $node->{lhs}->{'$or'} ) ) {
        push( @{ $node->{lhs}->{'$or'} }, $node->{rhs} );
        return $node->{lhs};
    }

    return { '$or' => [ $node->{lhs}, $node->{rhs} ] };
}

package Foswiki::Query::OP_not;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { '$not' => $node->{lhs} };
}

package Foswiki::Query::OP_gte;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { $node->{lhs} => { '$gte' => $node->{rhs} } };
}

package Foswiki::Query::OP_gt;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { $node->{lhs} => { '$gt' => $node->{rhs} } };
}

package Foswiki::Query::OP_lte;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { $node->{lhs} => { '$lte' => $node->{rhs} } };
}

package Foswiki::Query::OP_lt;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { $node->{lhs} => { '$lt' => $node->{rhs} } };
}

package Foswiki::Query::OP_match;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    my $rhs = quotemeta( $node->{rhs} );
    $rhs =~ s/\\\././g;
    $rhs =~ s/\\\*/*/g;
    $rhs = qr/$rhs/;

    return { $node->{lhs} => $rhs };
}

package Foswiki::Query::OP_ne;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { $node->{lhs} => { '$ne' => $node->{rhs} } };
}

package Foswiki::Query::OP_ob;
# ( )
sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return $node->{lhs};
}

package Foswiki::Query::OP_d2n;

package Foswiki::Query::OP_lc;

package Foswiki::Query::OP_length;

package Foswiki::Query::OP_ref;

package Foswiki::Query::OP_uc;

package Foswiki::Query::OP_where;

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
