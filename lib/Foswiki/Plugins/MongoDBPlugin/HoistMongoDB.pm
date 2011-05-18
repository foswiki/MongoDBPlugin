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

use constant MONITOR        => 0;
use constant MONITOR_DETAIL => 0;

=begin TML

---++ ObjectMethod hoist($query) -> $ref to IxHash


=cut

sub hoist {
    my ( $node, $indent ) = @_;

    print STDERR "HoistMongoDB::hoist from: ", $node->stringify(), "\n"
    #print STDERR "HoistMongoDB::hoist from: ", Dumper($node), "\n"
      if MONITOR
          or MONITOR_DETAIL;

    if (ref( $node->{op} ) eq '') {
        if (Foswiki::Func::isTrue($node->{params}[0])) {
            return {};
        } else {
            #TODO: or return false, or undef?
            return {'1' => '0'};
        }
    }

#TODO: use IxHash to keep the hash order - _some_ parts of queries are order sensitive
#    my %mongoQuery = ();
#    my $ixhQuery = tie( %mongoQuery, 'Tie::IxHash' );
#    $ixhQuery->Push( $scope => $elem );
    my $mongoDBQuery;

    $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::_hoist($node);

    if (
        ( ref($mongoDBQuery) ne '' )
        and (  defined( $mongoDBQuery->{'####need_function'} )
            or defined( $mongoDBQuery->{'####delay_function'} ) )
      )
    {

        #this can happen if the entire query is something like d2n(banana)
        #ideally, the parser should be converting that into a logical tree -
        #but for now, our parser is dumb, forcing hoisting code to suck
        print STDERR "\n......final convert..........\n" if MONITOR;
        $mongoDBQuery = { '$where' => convertToJavascript($mongoDBQuery) };
    }

    die $mongoDBQuery if ( ref($mongoDBQuery) eq '' );

#TODO: sadly, the exception throwing wasn't working so I'm using a brutish propogate error
    if ( defined( $mongoDBQuery->{ERROR} ) ) {
        print STDERR "AAAAARGH " . $mongoDBQuery->{ERROR} . "\n";
        return;
    }

    print STDERR "Hoisted to:  ",    #$node->stringify(), " -> /",
      Dumper($mongoDBQuery), "/\n"
      if MONITOR or MONITOR_DETAIL;

    if ( defined( $Foswiki::cfg{Plugins}{MongoDBPlugin}{UseJavascriptQuery} )
        and $Foswiki::cfg{Plugins}{MongoDBPlugin}{UseJavascriptQuery} )
    {

        #convert the entire query to a javascript $where clause for testing
        return { '$where' => convertToJavascript($mongoDBQuery) };
    }
    else {

#need to test to see if we need to re-write parts of the query in javascript
#ie, nested OR's
#one reason for doing it here, after we've made the mongo queries, is that later, they may implement it and we can remove the kludge
        kludge($mongoDBQuery);
    }
    $mongoDBQuery->{'$where'} = "".$mongoDBQuery->{'$where'} if (defined($mongoDBQuery->{'$where'}));

    return $mongoDBQuery;
}

sub kludge {
    my $node = shift;
    my $inOr = shift;

    foreach my $key ( keys(%$node) ) {
        my $value = $node->{$key};

        my $thisIsOr = ( $key eq '$or' );
        if ( $inOr and $thisIsOr ) {

            #nested OR detected, replace with $where and hope
            #try converting to $in first.

            my ( $lfield, $lhsIn, $lNum ) = convertOrToIn($value);
            if ( ( $lNum > 0 ) and !defined( $node->{$lfield} ) ) {
                $node->{$lfield} = $lhsIn;
            }
            else {
                $node->{'$where'} = convertToJavascript( { $key => $value } );
            }
            delete $node->{$key};
        }
        else {
            if ( ref($value) eq 'HASH' ) {
                kludge( $value, ( $inOr or $thisIsOr ) );
            }
            elsif ( ref($value) eq 'ARRAY' ) {
                next if ( $key eq '$in' );
                next if ( $key eq '$nin' );
                foreach my $n (@$value) {
                    kludge( $n, ( $inOr or $thisIsOr ) );
                }
            }
            else {
            }
        }
    }
}

sub _hoist {
    my $node = shift;
    my $level = shift || '';

    die 'node eq undef' unless defined($node);

    print STDERR "HoistMongoDB::hoist from: ", $node->stringify(),
      " (ref() == " . ref($node) . ")\n"
      if MONITOR
          or MONITOR_DETAIL;

    #forward propogate that we're inside a 'where' - eg lhs[rhs]
    #sadly, also need to treat a dot case : preferences[value=12].Red
    #   which is the same as preferences[value=12 AND name='Red']
    if ( ( ref( $node->{op} ) eq 'Foswiki::Query::OP_where' )
        or defined( $node->{inWhere} ) )
    {
        $node->{params}[0]->{inWhere} = $node->{inWhere}
          if ( defined( $node->{params}[0] )
            and ( ref( $node->{params}[0] ) ne '' ) );
        $node->{params}[1]->{inWhere} = ( $node->{inWhere} || $node )
          if ( defined( $node->{params}[1] )
            and ( ref( $node->{params}[1] ) ne '' ) );
    }
    print STDERR $level
      . "???????"
      . ref( $node->{op} ) . " "
      . ( $node->{inWhere} ? 'inWhere' : '' ) . "\n"
      if MONITOR
          or MONITOR_DETAIL;

    #optimise lc(rhs) = lc(lhs) so that we don't have to goto javascript
    #imo we should put this kind of optimisation into the parser
    if (
        (

            #TODO:        ( ref( $node->{op} ) eq 'Foswiki::Query::OP_eq' ) or
            ( ref( $node->{op} ) eq 'Foswiki::Query::OP_match' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_like' )

            #others?
        )
        and (    #TODO: uc()
            ( ref( $node->{params}[0]->{op} ) eq 'Foswiki::Query::OP_lc' )
            and ( ref( $node->{params}[1]->{op} ) eq 'Foswiki::Query::OP_lc' )
        )
      )
    {

        #redo it as a case insensitive regex.
        $node->{params}[0]   = $node->{params}[0]->{params}[0];
        $node->{params}[1]   = $node->{params}[1]->{params}[0];
        $node->{insensitive} = 1;
    }

    #name, or constants.
    if ( not ref( $node->{op} ) ) {

        #use Data::Dumper;
        print STDERR "not an op (" . Dumper($node) . ")\n" if MONITOR;
        return Foswiki::Query::OP_dot::hoistMongoDB( $node->{op}, $node );
    }
    my $unreality_arity = $node->{op}->{arity};
    $unreality_arity = scalar( @{ $node->{params} } )
      if ( $node->{op}->{canfold} );
    print STDERR "unreality_arity (" . $unreality_arity . ")\n" if MONITOR;

    if ( ref( $node->{op} ) eq 'Foswiki::Query::OP_dot' ) {

        if ( ref( $node->{params}[0]->{op} ) eq 'Foswiki::Query::OP_where' ) {

            #print STDERR "erkle ".Dumper($node->{params}[0])."\n";
            print STDERR "pre erkle::hoist from: ", $node->stringify(), "\n"
              if MONITOR
                  or MONITOR_DETAIL;

            my $rhs_Id = $node->{params}[1]->{params}[0];
            if
              ( #TODO: this is why you can't do this - if the post . portion is an attr name, its not a where selector
                ( $rhs_Id ne 'name' ) and ( $rhs_Id ne 'value' )
              )
            {

  #this is some pretty horrid mucking with reality
  #the rhs of this OP_dot needs to go inside the OP_Where stuff
  #as a name='$rhsval'
  #$node->{op}->{arity}--; - OK - so if you do this you are breaking everything.
                $unreality_arity--;

#TODO: Note - you can't do this - as it won't work if the $name is a registered attr
                my $eq = new Foswiki::Query::OP_eq();
                my $name_node =
                  Foswiki::Query::Node->newLeaf( 'name',
                    Foswiki::Infix::Node::NAME );
                my $eq_node =
                  Foswiki::Query::Node->newNode( $eq,
                    ( $name_node, $node->{params}[1] ) );
                my $and    = new Foswiki::Query::OP_and();
                my @params = $node->{params}[0]->{params}[1];
                push( @params, $eq_node );
                my $and_node =
                  Foswiki::Query::Node->newNode( $and,
                    $node->{params}[0]->{params}[1], $eq_node );

                $node->{params}[0]->{params} =
                  [ $node->{params}[0]->{params}[0], $and_node ];

                print STDERR "POST erkle::hoist from: ", $node->stringify(),
                  "\n"
                  if MONITOR
                      or MONITOR_DETAIL;
            }

            my $query = _hoist( $node->{params}[0], $level . ' ' );
            print STDERR "return 1\n" if MONITOR;
            return $query;
        }
        elsif ( ( ref( $node->{params}[0]->{op} ) eq '' )
            and ( ref( $node->{params}[1]->{op} ) eq '' ) )
        {

            #TODO: really should test for 'simple case' and barf elsewise
            print STDERR "return 2\n" if MONITOR;
            return Foswiki::Query::OP_dot::hoistMongoDB( $node->{op}, $node );
        }
        else {

            #die 'rogered';
        }
    }

    my $containsQueryFunctions = 0;

    #TODO: if 2 constants(NUMBER,STRING) ASSERT
    #TODO: if the first is a constant, swap
    for ( my $i = 0 ; $i < $unreality_arity ; $i++ ) {
        print STDERR "arity $i of $unreality_arity\n" if MONITOR;
        $node->{ 'hoisted' . $i } = _hoist( $node->{params}[$i], $level . ' ' );
        if ( ref( $node->{ 'hoisted' . $i } ) ne '' ) {

            print STDERR "ref($node->{'hoisted'.$i}) == "
              . ref( $node->{ 'hoisted' . $i } ) . "\n"
              if MONITOR;
            $node->{ERROR} = $node->{ 'hoisted' . $i }->{ERROR}
              if ( defined( $node->{ 'hoisted' . $i }->{ERROR} ) );
            $containsQueryFunctions |=
              defined( $node->{ 'hoisted' . $i }->{'####need_function'} );
            $node->{'####delay_function'} =
              $i . $node->{ 'hoisted' . $i }->{'####delay_function'}
              if (
                defined( $node->{ 'hoisted' . $i }->{'####delay_function'} ) );
        }
    }

    #monitor($node) if MONITOR;

    if ( defined( $node->{'####delay_function'} ) ) {

  #if we're maths, or a brace, return $node, and go for a further delay_function
        if (
               ( ref( $node->{op} ) eq 'Foswiki::Query::OP_ob' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_minus' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_plus' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_times' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_div' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_pos' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_neg' )

            #or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_ref' )
          )
        {

            print STDERR "return 3\n" if MONITOR;

            return $node->{op}->hoistMongoDB($node);

        }
        else {

            #else, set $containsQueryFunctions
            $containsQueryFunctions = 1;
        }
    }

#DAMMIT, I presume we have oddly nested eval/try catch so throwing isn't working
#throw Error::Simple( 'failed to Hoist ' . ref( $node->{op} ) . "\n" )
# unless ( $node->{op}->can('hoistMongoDB') );

    unless ( $node->{op}->can('hoistMongoDB') ) {
        $node->{ERROR} = 'can\'t Hoist ' . ref( $node->{op} );
    }
    if ( defined( $node->{ERROR} ) ) {
        print STDERR "HOIST ERROR: " . $node->{ERROR};
        die "HOIST ERROR: " . $node->{ERROR};
        return $node;
    }

    print STDERR "GIBBER" . $node->stringify() . "\n" if MONITOR;
    print STDERR "..............(" . Dumper($node) . ")\n" if MONITOR;

    #need to convert to js for lc/uc/length  etc :(
    # '####need_function'
    if ($containsQueryFunctions) {
        if ( ref( $node->{hoisted0} ) eq 'HASH' ) {
            if ( defined( $node->{hoisted0}->{'####delay_function'} ) ) {
                print STDERR "--- qwe \n"
                  if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;

                $node->{'####delay_function'} =
                  'k' . $node->{hoisted0}->{'####delay_function'};
            }
            print STDERR "--- asd \n"
              if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;
            $node->{hoisted0} = convertToJavascript( $node->{hoisted0} );
        }

        my $hoistedNode;
        if ( ref($node) eq 'Foswiki::Query::Node' ) {
            print STDERR 'who ' . Dumper($node)
              if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;
            $hoistedNode = $node->{op}->hoistMongoDB($node);
        }
        else {

#generally only happens if there is no rhs really (eg, a query that lookd like "d2n(SomeField)")
            $hoistedNode = $node;
            print STDERR 'norhs ' . Dumper($hoistedNode)
              if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;
            return $hoistedNode;
        }
        if ( ref($hoistedNode) eq '' ) {

            #could be a maths op - in which case, eeek?
            #shite - or maths inside braces
            print STDERR 'notahash ' . Dumper($hoistedNode)
              if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;
            return $hoistedNode;
        }
        else {

            #this is used to convert something like "lc(Subject)='webhome'"
            print STDERR 'why ' . Dumper($hoistedNode)
              if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;

#TODO: need to minimise the #delay_function setting - atm its a little over-enthusiastic
            return {
                '$where'             => convertToJavascript($hoistedNode),
                '####delay_function' => 'p'
                  . (
                    defined( $hoistedNode->{'####delay_function'} )
                    ? $hoistedNode->{'####delay_function'}
                    : 99
                  )
            };
        }
    }
    return $node->{op}->hoistMongoDB($node);
}

sub monitor {
    my $node = shift;

    print STDERR "MONITOR Hoist node->op="
      . Dumper( $node->{op} )
      . " ref(node->op)="
      . ref( $node->{op} ) . "\n";

    print STDERR "\nparam0(" . $node->{params}[0]->{op} . "): ",
      Data::Dumper::Dumper( $node->{params}[0] ), "\n";
    if ( $node->{op}->{arity} > 1 ) {
        print STDERR "\nparam1(" . $node->{params}[1]->{op} . "): ",
          Data::Dumper::Dumper( $node->{params}[1] ), "\n";
    }

    #TODO: mmm, do we only have unary and binary ops?

    print STDERR "----lhs: " . Data::Dumper::Dumper( $node->{hoisted0} );
    if ( $node->{op}->{arity} > 1 ) {
        print STDERR "----rhs: " . Data::Dumper::Dumper( $node->{hoisted1} );
    }
    print STDERR " \n";

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

my %js_func_map = (
    '#lc'     => 'foswiki_toLowerCase',
    '#uc'     => 'foswiki_toUpperCase',
    '#length' => 'foswiki_length',
    '#d2n'    => 'foswiki_d2n',
    '#int'    => 'parseInt',
    '#match'  => 'MATCHBANG',
    '#like'   => 'LIKEBANG',
    '#div'    => '/',
    '#mult'   => '*',
    '#plus'   => '+',
    '#minus'  => '-',
    '#pos'    => '+',
    '#neg'    => '-',
    '#ref'    => 'REF'
);

sub convertFunction {
    my ( $value, $key ) = @_;
    
    if (   ( $key eq '#lc' )
        or ( $key eq '#uc' )
        or ( $key eq '#length' )
        or ( $key eq '#d2n' )
        or ( $key eq '#int' ) )
    {
        return $js_func_map{$key} . '(' . convertStringToJS($value) . ')';
    }
    if ( $key eq '#match' ) {
        my $regex        = convertStringToJS($value);
        my $regexoptions = '\'\'';
        return "Regex($regex, $regexoptions).test";    #(this.\$scope);";
    }
    if ( $key eq '#like' ) {
        my $regex        = convertStringToJS($value);
        my $regexoptions = '\'\'';
        return "Regex('^'+$regex+'$', $regexoptions).test";   #(this.\$scope);";
    }
    if ( $key eq '#ref' ) {

        #TODO: like all accessses, this needs alot of undef protection.
        my $addr = '(' . convertStringToJS( $$value[1] ) . ')';
##%TMPL:DEF{foswiki_getRef_js}%function(host, collection, currentqueryweb, topic) {
        my $ref =
'foswiki_getRef(\'localhost\', foswiki_getDatabaseName(this._web), \'current\', this._web, '
          . convertStringToJS( $$value[0] ) . ')';
          
        ASSERT($addr =~ /this/) if DEBUG
          
        $addr =~ s/this/$ref/;
        return $addr;
    }
    if (   ( $key eq '#div' )
        or ( $key eq '#mult' )
        or ( $key eq '#plus' )
        or ( $key eq '#minus' )
        or ( $key eq '#pos' )
        or ( $key eq '#neg' ) )
    {

        return
            '('
          . convertStringToJS( $$value[0] ) . ')'
          . $js_func_map{$key} . '('
          . convertStringToJS( $$value[1] ) . ')';
    }
    die "$key and $value is not a string? " if ( ref($value) ne '' );
    die "$key is not in the js_func_map"
      if ( not defined( $js_func_map{$key} ) );
    print STDERR "\t\tconvertfunction($value, $key) => \n" if MONITOR;
    return convertStringToJS($value) . $js_func_map{$key};
}

my $fields = '(' . join( '|', keys(%Foswiki::Meta::VALIDATE) ) . ')';
my $ops    = '(' . join( '|', values(%js_op_map) ) . ')';

sub convertStringToJS {
    my $string = shift;
    print STDERR "  convertStringToJS($string)\n" if MONITOR;

    return $string
      if ( $string =~ /^\(.*\)$/ )
      ;    #if we're doing braces, and they're not quoted leave things be

    return convertToJavascript($string) if ( ref($string) eq 'HASH' );

    #foswiki special constants: undefined, now.. ?
    return $string if ($string eq 'null');

    return $string if ( $string =~ /^'.*'^/ );

#TODO: i _think_ the line below is ok, its needed to make ::test_hoistLengthLHSString work
    return $string if ( $string =~ /^\'.*/ );

    # all registered meta type prefixes use a this. in js
    return $string if ( $string =~ /^foswiki_/ );
    return 'foswiki_getField(this, \'' . $string . '\')'
      if ( $string =~ /^$fields/ );

    return $string
      if ( $string =~ /^$ops$/ );    #for ops, we only want the entirety

    return $js_op_map{$string} if ( defined( $js_op_map{$string} ) );
    return $string if ( $string =~ /^\s?\(?Regex.*/ );

    #TODO: generalise - we should not clobber over js_func_map values
    return $string if ( $string =~ /^foswiki_d2n\(.*/ );
    return $string if ( $string eq '<' );

    return 'this.' . $string if ( $string eq '_web' );
    return 'this.' . $string if ( $string eq '_topic' );
    return 'this.' . $string if ( $string eq '_text' );

#if it looks like a number, lets try treating it like a number, and see what happens
#i _think_ this will result in js doing magic just like perl does, as the main diff seems to be that Perl('1'+'1'=2) and JS('1'+'1'='11')
    return $string if ( $string =~ /^[+-]?\d+(\.\d*)?$/ );

    return '\'' . $string . '\'';
}

#converts a mongodb query hash into a $where clause
#frustratingly, both (field:value) and {field:{$op,value}} and {$op, {nodes}} and {$op, []} are valid, making it a little messy
#and {$op, []} might refer to the key outside it (as in $nin), or not, as in $or
sub convertToJavascript {
    my $node      = shift;
    my $statement = '';

    ASSERT( ref($node) eq 'HASH' ) if DEBUG;

#do nothing, if there is only a $where and #need_function and/or #delay_function
    my @keys     = keys(%$node);
    my $keycount = $#keys;
    $keycount-- if ( defined( $node->{'####need_function'} ) );
    $keycount-- if ( defined( $node->{'####delay_function'} ) );
    return $node->{'$where'}
      if ( ( $keycount == 0 ) and ( defined( $node->{'$where'} ) ) );

#TODO: for some reason the Dumper call makes the HoistMongoDBsTests::test_hoistLcRHSName test succeed - have to work out what i've broken.
    my $dump = Dumper($node);

    print STDERR "\n..............convertToJavascript " . Dumper($node) . "\n"
      if MONITOR;

    while ( my ( $key, $value ) = each(%$node) ) {
        next if ( $key eq '####need_function' );
        next if ( $key eq '####delay_function' );
        $statement .= ' && ' if ( $statement ne '' );

    #BEWARE: if ref($node->{hoisted0}) eq 'HASH' then $key is going to be wrong.
        if ( ref( $node->{hoisted0} ) eq 'HASH' ) {
            die 'unexpectedly';
            $key = convertToJavascript( $node->{hoisted0} );
        }

        #convert $ops into js ones
        my $js_key = $key;

        $js_key = $js_op_map{$key} if ( defined( $js_op_map{$key} ) );
        print STDERR "key = $key ("
          . ref($js_key)
          . ", $js_key), value = $value\n"
          if MONITOR;

        if ( ref($value) eq 'HASH' ) {
            my ( $k, $v ) = each(%$value);
            if ( ( $k eq '$in' ) or ( $k eq '$nin' ) ) {

#TODO: look up to see if javascript thas an value.in(list) or ARRAY.contains(value)
                $statement .=
                  ( ( $k eq '$nin' ) ? '!' : ' ijij ' ) . ' ( ' . join(
                    ' || ',
                    map {
                            convertStringToJS($js_key) . ' == '
                          . convertStringToJS($_)
                      } @$v
                  ) . ' ) ';
            }
            elsif ( $key =~ /^\#/ ) {

                $statement .= convertFunction( $value, $key );
            }
            elsif ( $k eq '#match' ) {

                #this is essentially an operator lookahead
                print STDERR ">>>>>>>>>>>>>>>>>>>> lookahead $key -> $k\n"
                  if MONITOR;
                $statement .=
                    convertToJavascript($value) . '('
                  . convertStringToJS($js_key) . ')';
            }
            elsif (( $k eq '#like' )
                or ( $k eq '#div' )
                or ( $k eq '#mult' )
                or ( $k eq '#plus' )
                or ( $k eq '#minus' )
                or ( $k eq '#pos' )
                or ( $k eq '#neg' ) )
            {

                #this is essentially an operator lookahead
                print STDERR ">>>>>>>>>>>>>>>>>>>> lookahead $key -> $k\n"
                  if MONITOR;
                $statement .= ($js_key) . ' ' . convertStringToJS($value);

#$statement .= convertFunction( convertToJavascript($value), $key );
#                $statement .=
#                  convertToJavascript($value).'('.convertStringToJS($js_key).')';
            }
            elsif ( ( $key eq '$not' ) ) {

                #this is essentially an operator lookahead
                $statement .=
                    convertStringToJS($js_key) . ' ( '
                  . convertToJavascript($value) . ' ) ';
            }
            elsif ( ( $k =~ /^\$/ ) ) {

                #this is essentially an operator lookahead
                $statement .=
                    convertStringToJS($js_key) . ' '
                  . convertToJavascript($value);
            }
            elsif ( $key =~ /^\$/ and defined( $js_op_map{$key} ) ) {
                $statement .= ($js_key) . ' ' . convertStringToJS($value);
            }
            else {
                $statement .=
                    convertStringToJS($js_key) . ' == '
                  . convertToJavascript($value);
            }
        }
        elsif ( ref($value) eq 'ARRAY' ) {
            if ( ( $key eq '$in' ) or ( $key eq '$nin' ) ) {
                die 'unpleasently'
                  ; #should never get here - it needs to be handled while we knoe the field it refers to
            }
            elsif ( ( $key eq '$or' ) or ( $key eq '$nor' ) ) {

                $js_key = '$or' if ( $key eq '$nor' );

                #er, assuming $key == $or - $in and $nin will kick me
                my $new = ' ( '
                  . join(
                    ' ' . convertStringToJS($js_key) . ' ',
                    map { convertToJavascript($_) } @$value
                  ) . ' ) ';
                $new = '(!' . $new . ')' if ( $key eq '$nor' );

                $statement .= $new;
            }
            elsif ( $key =~ /^#/ ) {

                #maths ops
                $statement .= convertFunction( $value, $key );
            }
            else {
                die 'sadly ' . $key;
            }

        }
        else {
            if ( $key eq '$where' ) {

                $statement .= " ($value) ";
            }
            elsif ( $key eq '#where' ) {

                $statement .= " ($value) ";
            }
            else {

                #value isa string..
                #TODO: argh, string or number, er, or regex?
                print STDERR "convertToJavascript - $key => $value is a "
                  . ref($value) . "\n"
                  if MONITOR;
                if ( ref($value) eq 'Regexp' ) {
                    $value =~
                      /\(\?[xism]*-[xism]*:(.*)\)/;    #TODO: er, regex options?
                    $statement .=
                      "( /$1/.test(" . convertStringToJS($js_key) . ") )";

                }
                elsif ( $key =~ /^\#/ ) {
                    print STDERR ">>>>>>>>>>>>>>>>>>>> #hash - $key \n"
                      if MONITOR;

                    $statement .= convertFunction( $value, $key );
                }
                elsif ( $key =~ /^\$/ and defined( $js_op_map{$key} ) ) {
                    $statement .= ($js_key) . ' ' . convertStringToJS($value);
                }
                else {

#TODO: can't presume that the 'value' is a constant - it might be a META value name
                    $statement .=
                        convertStringToJS($js_key) . ' == '
                      . convertStringToJS($value);
                }
            }
        }
    }
    print STDERR "----returning $statement\n" if MONITOR;
    $statement = "$statement";  #make sure we're a string at this point
    return $statement;
}

#---+++ convertOrToIn($refToOrArray) -> ($field, $mongoInQuery, numberOfElements)
#if we can convert to in / nin, do so, else return undef
#return partial query hash - ie {$in : []} or {$nin : []}
sub convertOrToIn {
    my $orArrayRef = shift;

    my $fieldname;
    my $mongoInQuery;
    my $numberOfElements = 0;

    my $failedToConvertToOnlyIn = 0;

#if more than 2 elements in the ARRAY are the same key, and same negation, aggregate into $in / $nin
#IFF the elements in the array are simple - otherwise, need to kick out
    my $keys = {};
    my @complex;
    foreach my $elem (@$orArrayRef) {
        $numberOfElements++;
        my ( $firstKey, $moreKeys ) = keys(%$elem);
        my $firstVal = $elem->{$firstKey};
        if ( defined($moreKeys)
            or not( ( ref($firstVal) eq '' ) or ( ref($firstVal) eq 'Regexp' ) )
          )
        {
            print STDERR "------ too complex ($firstKey,$firstVal == "
              . ref($firstVal) . ")\n"
              if MONITOR;

            #actually, if its an $in/$nin we could do something..
            push( @complex, $elem );
            $failedToConvertToOnlyIn = 1;
        }
        else {

            #just do $in for now
            print STDERR "------ simple ($firstKey, $firstVal)\n" if MONITOR;
            push( @{ $keys->{$firstKey} }, $firstVal );
        }
    }

    while ( my ( $k, $v ) = each(%$keys) ) {
        my @array = @$v;

        print STDERR "------ erg $#array\n" if MONITOR;

        if ( $#array >= 1 ) {
            print STDERR "------ $k => \$in @$v\n" if MONITOR;

            push( @complex, { $k => { '$in' => $v } } );
        }
        else {
            print STDERR "------ $k => $v->[0]\n" if MONITOR;
            push( @complex, { $k => $v->[0] } );
        }
    }

    if ( scalar(@complex) > 1 ) {
        $failedToConvertToOnlyIn = 1;

        #$optimisedOr->{'$or'} = \@complex;
    }
    else {

        #success - its an $in?
        ( $fieldname, $mongoInQuery ) = each( %{ $complex[0] } );
    }

    #use Data::Dumper;
    #print STDERR
    #  "----------------------------------------------------- STUPENDIFY!! - ("
    #  . Dumper($mongoInQuery) . ")\n"
    #  if MONITOR;
    return ( $fieldname, $mongoInQuery,
        ( $failedToConvertToOnlyIn ? 0 : $numberOfElements ) );
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
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );
    ASSERT( ref( $node->{hoisted0} ) eq '' );

    #ASSERT(ref($node->{hoisted1}) eq '');

    ASSERT( $node->{op}->{name} eq '=' ) if DEBUG;
    
    #TODO: i think there are other cases that will pop up as 'needs js'
    #TODO: see above, where we should 'optimise' so that if there is a constant on the lhs, and a meta feild on the rhs, that we swap..
    if (
        ($node->{hoisted0} eq 'null') or 
        ($node->{hoisted1} eq 'null')       #TODO: need to find the mongoquery way to test for undefined.
       ) {
        return {
            '$where' =>
              Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                { $node->{hoisted0} => $node->{hoisted1} }
              )
        };
    }
    
    return { $node->{hoisted0} => $node->{hoisted1} };
}

package Foswiki::Query::OP_like;
use Assert;

#hoist ~ into a mongoDB ixHash query

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    if ( ref( $node->{hoisted1} ) ne '' ) {

        die 'not implemented yet' if ( defined( $node->{insensitive} ) );

        #            die 'er, no, can\'t regex on a function';
        # re-write one as $where
        return {
            '$where' =>
              Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                { $node->{hoisted0} => { '#like' => $node->{hoisted1} } }
              )
        };
    }

    my $rhs = quotemeta( $node->{hoisted1} );
    $rhs =~ s/\\\?/./g;
    $rhs =~ s/\\\*/.*/g;
    
    if ( defined( $node->{insensitive} ) ) {
        $rhs = qr/^$rhs$/im;
    }
    else {
        $rhs = qr/^$rhs$/m;
    }

    return { $node->{hoisted0} => $rhs };
}

package Foswiki::Query::OP_match;
use Assert;

#hoist =~ into a mongoDB ixHash query

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    if ( ref( $node->{hoisted1} ) ne '' ) {

        #            die 'er, no, can\'t regex on a function';
        # re-write one as $where

        die 'not implemented yet' if ( defined( $node->{insensitive} ) );

        return {
            '$where' =>
              Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                { $node->{hoisted0} => { '#match' => $node->{hoisted1} } }
              )
        };
    }

    my $rhs = quotemeta( $node->{hoisted1} );
    $rhs =~ s/\\\././g;
    $rhs =~ s/\\\*/*/g;

    #marginal speedup, but still every straw
    return {} if ( $rhs eq '.*' );

    if ( defined( $node->{insensitive} ) ) {
        $rhs = qr/$rhs/i;
    }
    else {
        $rhs = qr/$rhs/;
    }

    return { $node->{hoisted0} => $rhs };
}

=begin TML

---++ ObjectMethod Foswiki::Query::OP_dot::hoistMongoDB($node) -> $ref to IxHash


=cut

package Foswiki::Query::OP_dot;
use Foswiki::Meta;
use Assert;

#mongo specific aliases
our %aliases = (
    name => '_topic',
    web  => '_web',
    text => '_text',
    undefined => 'null',
    now => time()
);

sub mapAlias {
    my $name = shift;
    ASSERT( ( ref($name) eq '' ) );

    #TODO: map to the MongoDB field names (name, web, text, fieldname)
    if ( defined( $aliases{$name} ) ) {
        $name = $aliases{$name};
    }
    elsif ( defined( $Foswiki::Query::Node::aliases{$name} ) ) {
        $name = $Foswiki::Query::Node::aliases{$name};
    }
    $name =~ s/^META://;

    return $name;
}

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    if ( not defined( $node->{op} ) ) {
        print STDERR 'CONFUSED: ' . Data::Dumper::Dumper($node) . "\n"
          if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;
        die 'here';

        #return;
    }

#TODO: both the next 2 should only work form registered META - including what params are allowed. (except we don't do that for FIELDS :(
    if ( ref( $node->{op} ) ) {

        my $dont_know_why;

        #an actual OP_dot
        my $lhs = $node->{params}[0];
        my $rhs = $node->{params}[1];
        if ( ref( $rhs->{op} ) ne '' ) {
            print STDERR "------------rhs =="
              . Data::Dumper::Dumper($rhs) . "\n"
              if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;
            $rhs = $node->{hoisted1};
        }
        else {
            $rhs = $node->{params}[1]->{params}[0];
        }
        if ( ref( $lhs->{op} ) ne '' ) {
            print STDERR "------------lhs =="
              . Data::Dumper::Dumper($lhs)
              . "\nOOO "
              . ref( $lhs->{op} )
              . " OOO \n"
              if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR_DETAIL;

            #return $node->{op}->hoistMongoDB($node);
            if (    ref($lhs) ne ''
                and ref( $lhs->{op} ) eq 'Foswiki::Query::OP_ref' )
            {    # and defined($node->{hoisted0}->{'#ref'})) {
                print STDERR "+_+_+_+_+_+_+_+_+_+_+_+_+_+_GIMPLE($rhs)("
                  . Data::Dumper::Dumper($lhs) . ") => "
                  . $lhs->{hoisted1} . '.'
                  . $node->{params}[1]->{params}[0] . "\n"
                  if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;
                return {
                    '#ref' => [
                        $lhs->{hoisted0},
                        $lhs->{hoisted1} . '.' . $node->{params}[1]->{params}[0]
                    ],
                    '####delay_function' => 13
                };
                $lhs->{hoisted1} .= '.' . $node->{hoisted1};
                return {
                    '#ref' => [ $lhs->{hoisted0}, $lhs->{hoisted1} ],
                    '####delay_function' => 13
                };
            }
            $lhs = $node->{hoisted0};
        }
        else {
            $lhs = $node->{params}[0]->{params}[0];
        }

        print STDERR "-------------------------------- hoist OP_dot("
          . ref( $node->{op} ) . ", "
          . Data::Dumper::Dumper($node) . ")\n"
          if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;

        #an actual OP_dot
        ASSERT( ref($lhs) eq '' );
        ASSERT( ref($rhs) eq '' );

        my $mappedName = mapAlias($lhs);

        print STDERR "-INTO " . $mappedName . '.' . $rhs . "\n"
          if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;

        if ( $mappedName ne $lhs ) {
            return $mappedName . '.' . $rhs;
        }
        else {

            # TODO: assumes the term before the dot is the form name??? gads
            return 'FIELD.' . $rhs . '.value';
        }
    }
    elsif ( $node->{op} == Foswiki::Infix::Node::NAME ) {

        #if we're in a where, this is a bit transmissive
        print STDERR "============================= hoist OP_dot("
          . $node->{op} . ", "
          . $node->{params}[0] . ', '
          . ( defined( $node->{inWhere} ) ? 'inwhere' : 'notinwhere' ) . ")\n"
          if Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::MONITOR;

      #if we're in a 'where' eg preferences[name = 'Summary'] then don't aliases
        return $node->{params}[0] if ( defined( $node->{inWhere} ) );

        #if its a registered META, just return it.
        if ( $node->{params}[0] =~ /META:(.*)/ ) {
            return $1 if ( defined( $Foswiki::Meta::VALIDATE{$1} ) );
        }

        my $mappedName = mapAlias( $node->{params}[0] );
        if ( $mappedName ne $node->{params}[0] ) {
            $mappedName =~ s/^META://;
            return $mappedName;
        }
        else {

            #no idea - so we treat it like a field
            return 'FIELD.' . $node->{params}[0] . '.value';
        }
    }
    elsif ( $node->{op} == Foswiki::Infix::Node::NUMBER ) {

        #TODO: would love to convert to numbers, don't think i can yet.
        return $node->{params}[0];
    }
    elsif ( $node->{op} == Foswiki::Infix::Node::STRING ) {
        return $node->{params}[0];
    }
}

package Foswiki::Query::OP_and;
use Foswiki::Plugins::MongoDBPlugin::HoistMongoDB;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

#beware, can't have the same key in both lhs and rhs, as the hash collapses them into one
#this is more a limitation of the mongodb drivers - internally, mongodb (i'm told) can doit.

    my %andHash;
    if ( ref( $node->{hoisted0} ) eq 'HASH' ) {
        %andHash = %{ $node->{hoisted0} };
    }
    else {
        $andHash{'$where'} =
          Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
            { '#where' => $node->{hoisted0} } );
    }
    my $i = 1;
    while ( defined( $node->{ 'hoisted' . $i } ) ) {
        if ( ref( $node->{ 'hoisted' . $i } ) eq '' ) {
            $node->{ 'hoisted' . $i } = {
                '$where' =>
                  Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertStringToJS(
                    $node->{ 'hoisted' . $i }
                  )
            };
        }
        foreach my $key ( keys( %{ $node->{ 'hoisted' . $i } } ) ) {
            if ( defined( $andHash{$key} ) ) {
                my $conflictResolved = 0;
                if ( $key =~ /^\$.*/ ) {

                    #its an operator $or, $in, $nin,
                    if ( $key eq '$or' ) {

        #if one of the OR's happens to be all on the same field, then its simple
        #if not, (A OR B) AND (C OR D) == (A OR B) AND NOT (NOT C AND NOT D) :(
        #EXCEPT of course, that mongo doesn't have NOT. - but... we do have $nor
        #A AND B == NOT (NOT A OR NOT B) == $nor: {A $ne banana, B $ne trifle}

                        my ( $rfield, $rhsIn, $rNum ) =
                          Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertOrToIn(
                            $node->{ 'hoisted' . $i }->{$key} );
                        my ( $lfield, $lhsIn, $lNum ) =
                          Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertOrToIn(
                            $andHash{$key} );

                        if ( ( $rNum == 0 ) and ( $lNum == 0 ) ) {
                            die 'I have no solution for this query';
                        }
                        else {
                            if ( ( $rNum > 0 ) and ( $lNum > 0 ) ) {
                                if ( $rfield eq $lfield ) {

                                    #bummer, have to choose one or the other
                                    if ( $rNum > $lNum ) {
                                        $andHash{$rfield} = $rhsIn;
                                    }
                                    else {
                                        $andHash{$lfield} = $lhsIn;
                                        $andHash{'$or'} =
                                          $node->{ 'hoisted' . $i }->{$key};
                                    }
                                }
                                else {
                                    $andHash{$rfield} = $rhsIn;
                                    $andHash{$lfield} = $lhsIn;
                                    delete $andHash{'$or'};
                                }
                            }
                            elsif ( $rNum > 0 ) {
                                $andHash{$rfield} = $rhsIn;
                            }
                            else {
                                $andHash{$lfield} = $lhsIn;
                                $andHash{'$or'} =
                                  $node->{ 'hoisted' . $i }->{$key};
                            }
                            $conflictResolved = 1;
                        }

                    }
                    else {

                   #i think it should be ok, but i've not seen it happen, so die
                        die '---not here yet ' . $key if ( $key ne '$where' );
                    }
                }
                else {

                    #mmmm, SomeField == '1234' AND SomeField == '2345'?
                    #work out if its true or false, and work out something

    #TODO: how about minimising 2 identical non-simple queries ANDed?
    #this eq test below doesn't do identical regex, nor identical $ne etc..
    #                    use Data::Dumper;
    #                         print STDERR "----+++++++++++++++++ $key ||"
    #                           . Dumper($andHash{$key}) . "||"
    #                           . Dumper($node->{'hoisted'.$i}->{$key}) . "||"
    #                           . ref( $andHash{$key} ) . "||"
    #                           . ref( $node->{'hoisted'.$i}->{$key} ) . "||\n";

                    if (    ( ref( $andHash{$key} ) ne 'HASH' )
                        and
                        ( ref( $node->{ 'hoisted' . $i }->{$key} ) ne 'HASH' ) )
                    {

                        #simplest case - both are implicit 'eq'

                        if ( $andHash{$key} eq
                            $node->{ 'hoisted' . $i }->{$key} )
                        {

                            #they're the same, ignore the second..
                            $conflictResolved = 1;

                        }
                        else {

    #same field being tested in AND - have no choice but to goto $where
    #print STDERR "sameosameo ($key) is a hash on both sides - not here yet \n";
                        }
                    }
                    elsif (
                        (
                            ref( $andHash{$key} ) eq
                            'HASH' ) #if we're already in a non-trivial compare.
                        and
                        ( ref( $node->{ 'hoisted' . $i }->{$key} ) eq 'HASH' )
                      )
                    {
                        if (
                            ( defined( $andHash{$key}->{'$ne'} ) )
                            and (
                                defined(
                                    (
                                        $node->{ 'hoisted' . $i }->{$key}
                                          ->{'$ne'}
                                    )
                                )
                            )
                          )
                        {

                    #TODO: ERROR: this presumes that there isn't already a $nin.
                            ###(A != 'qe') AND (A != 'zx') transforms to {A: {$nin: ['qe', 'zx']}} (and regex $ne too?)
                            if ( not defined( $andHash{$key}->{'$nin'} ) ) {
                                $andHash{$key}->{'$nin'} = ();
                            }
                            push(
                                @{ $andHash{$key}->{'$nin'} },
                                $andHash{$key}->{'$ne'}
                            );
                            delete $andHash{$key}->{'$ne'};
                            push(
                                @{ $andHash{$key}->{'$nin'} },
                                $node->{ 'hoisted' . $i }->{$key}->{'$ne'}
                            );

                            $conflictResolved = 1;
                        }
                        else {

              #print STDERR "($key) is a hash on both sides - convert to js \n";
                        }
                    }
                    elsif ( ( ref( $andHash{$key} ) eq 'HASH' )
                        and
                        ( ref( $node->{ 'hoisted' . $i }->{$key} ) ne 'HASH' ) )
                    {

                      #$andHash{$key} complex - beware.
                      #$node->{'hoisted'.$i}->{$key} simple - can we toss at $in
                        if ( not defined( $andHash{$key}->{'$in'} ) ) {
                            $andHash{$key}->{'$in'} = ();
                        }
                        push(
                            @{ $andHash{$key}->{'$in'} },
                            $node->{ 'hoisted' . $i }->{$key}
                        );
                        $conflictResolved = 1;
                    }
                    elsif ( ( ref( $andHash{$key} ) ne 'HASH' )
                        and
                        ( ref( $node->{ 'hoisted' . $i }->{$key} ) eq 'HASH' ) )
                    {
                        $andHash{$key} = { '$in' => [ $andHash{$key} ] };
                        if (
                            defined(
                                $node->{ 'hoisted' . $i }->{$key}->{'$ne'}
                            )
                          )
                        {
                            $andHash{$key}->{'$nin'} =
                              [ $node->{ 'hoisted' . $i }->{$key}->{'$ne'} ];
                            $conflictResolved = 1;
                        }
                        if (
                            defined(
                                $node->{ 'hoisted' . $i }->{$key}->{'$not'}
                            )
                          )
                        {
                            $andHash{$key}->{'$nin'} =
                              [ $node->{ 'hoisted' . $i }->{$key}->{'$not'} ];
                            $conflictResolved = 1;
                        }
                    }
                    else {

                        print STDERR 'what are we ?';
                        die 'bonus2';
                    }
                }
                if ( not $conflictResolved ) {

                    if ( defined( $andHash{'$where'} ) ) {

                        # re-write one as $where
                        $andHash{'$where'} =
                          Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                            {
                                $key     => $node->{ 'hoisted' . $i }->{$key},
                                '#where' => $andHash{'$where'}
                            }
                          );
                    }
                    else {

                        # re-write one as $where
                        $andHash{'$where'} =
                          Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                            { $key => $node->{ 'hoisted' . $i }->{$key} } );
                    }

                }
            }
            else {
                $andHash{$key} = $node->{ 'hoisted' . $i }->{$key};
            }

        }
        $i++;
    }
    return \%andHash;
}

package Foswiki::Query::OP_or;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    my $mongoQuery;
    
    my @elements;
    my $i = 0;
    while (defined($node->{'hoisted'.$i})) {
        my $elem = $node->{'hoisted'.$i};
        if (defined($elem->{'$or'})) {
            #this might be un-necessary in the new nary node world
            my $ors = $elem->{'$or'};
            push(@elements, @$ors);
        } else {
            push(@elements, $elem);
        }
        $i++;
    }
    return {'$or' => \@elements};
}

package Foswiki::Query::OP_not;
use Foswiki::Plugins::MongoDBPlugin::HoistMongoDB;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' ) if DEBUG;
    ASSERT( not defined( $node->{hoisted1} ) ) if DEBUG;

    my %query;

    #no, $not is a dirty little thing that needs to go inside lhs :(
    #EXCEPT when we're doing AND
    my $lhs = $node->{hoisted0};

    if ( ( ref($lhs) eq '' ) or ( ref($lhs) eq 'Regexp' ) ) {

#TODO: this needs much more consideration :/
#need to activate the magic perl-er-isation of boolean - which I think means don't convert until the parent'
        $query{'$where'} =
          Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
            { '$not' => $lhs } );
    }
    elsif ( 1 == 2 ) {
        foreach my $key ( keys(%$lhs) ) {

#use Data::Dumper;
#print STDERR "###### OP_not ".Dumper($lhs->{$key})." - ".ref($lhs->{$key})."\n";
#TODO: convert $nin to $in, and vs versa
            if ( ref( $lhs->{$key} ) eq '' ) {

                #if this is a name / string etc - use $ne :(
                if ( $key eq '$where' ) {
                    $query{$key} = '!' . $lhs->{$key};
                }
                else {
                    $query{$key} = { '$ne' => $lhs->{$key} };
                }
            }
            else {
                if ( $key eq '$where' ) {
                    $query{$key} = '!' . $lhs->{$key};
                }
                else {
                    $query{$key} = { '$not' => $lhs->{$key} };
                }
            }
        }
    }
    else {
        my @keys = keys(%$lhs);
        if ( $#keys == 0 ) {
            if (   ( ref( $lhs->{ $keys[0] } ) eq '' )
                or ( ref( $lhs->{ $keys[0] } ) eq 'Regexp' ) )
            {
                if ( $keys[0] eq '$where' ) {
                    $query{'$where'} =
                      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                        { '$not' => $lhs } );
                }
                else {
                    if ( ref( $lhs->{ $keys[0] } ) eq '' ) {
                        return { $keys[0] => { '$ne' => $lhs->{ $keys[0] } } };
                    }
                    else {
                        return { $keys[0] => { '$not' => $lhs->{ $keys[0] } } };
                    }
                }
            }
            elsif ( $keys[0] eq '$or' ) {

                #TODO: avoid using $nor, and convert to $in and $nin
                return { '$nor' => $lhs->{ $keys[0] } };
            }
            else {
                return { $keys[0] => { '$not' => $lhs->{ $keys[0] } } };
            }
        }

        #esplode - need to convert to js :(
        return {
            '$where' =>
              Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                { '$not' => $lhs }
              )
        };
    }
    return \%query;
}

package Foswiki::Query::OP_gte;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node)               eq 'Foswiki::Query::Node' );
    ASSERT( ref( $node->{hoisted0} ) eq '' );
    ASSERT( ref( $node->{hoisted1} ) eq '' );

    return { $node->{hoisted0} => { '$gte' => $node->{hoisted1} } };
}

package Foswiki::Query::OP_gt;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return { $node->{hoisted0} => { '$gt' => $node->{hoisted1} } };
}

package Foswiki::Query::OP_lte;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node)               eq 'Foswiki::Query::Node' );
    ASSERT( ref( $node->{hoisted0} ) eq '' );
    ASSERT( ref( $node->{hoisted1} ) eq '' );

    return { $node->{hoisted0} => { '$lte' => $node->{hoisted1} } };
}

package Foswiki::Query::OP_lt;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );
    ASSERT( ref( $node->{hoisted0} ) eq '' );

    #    ASSERT(ref($node->{hoisted1}) eq '');

    return { $node->{hoisted0} => { '$lt' => $node->{hoisted1} } };
}

package Foswiki::Query::OP_ne;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    #ASSERT(ref($node->{hoisted0}) eq '');
    ASSERT( ref( $node->{hoisted1} ) eq '' );

    return { $node->{hoisted0} => { '$ne' => $node->{hoisted1} } };
}

package Foswiki::Query::OP_ob;
use Assert;

# ( )
sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return $node->{hoisted0};
}

package Foswiki::Query::OP_where;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

#AHA. this needs to use $elemMatch
#> t.find( { x : { $elemMatch : { a : 1, b : { $gt : 1 } } } } )
#{ "_id" : ObjectId("4b5783300334000000000aa9"),
#"x" : [ { "a" : 1, "b" : 3 }, 7, { "b" : 99 }, { "a" : 11 } ]
#}
#and thus, need to re-do the mongodb schema so that meta 'arrays' are arrays again.
#and that means the FIELD: name based shorcuts need to be re-written :/ de-indexing the queries :(

    if ( ref( $node->{hoisted0} ) ne '' ) {

#some darned bugger thought field[name="white"][value="black"] was worth parsing to

        #add $node->{hoisted1} to the lhs' $elemMatch
        my @k = keys( %{ $node->{hoisted0} } );
        my @v = each( %{ $node->{hoisted1} } );

        $node->{hoisted0}->{ $k[0] }->{'$elemMatch'}->{ $v[0] } = $v[1];

        return $node->{hoisted0};
    }

    return { $node->{hoisted0}
          . '.__RAW_ARRAY' => { '$elemMatch' => $node->{hoisted1} } };
}

package Foswiki::Query::OP_d2n;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return { '#d2n' => $node->{hoisted0}, '####need_function' => 1 };
}

package Foswiki::Query::OP_lc;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return { '#lc' => $node->{hoisted0}, '####need_function' => 1 };
}

package Foswiki::Query::OP_length;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return { '#length' => $node->{hoisted0}, '####need_function' => 1 };
}

package Foswiki::Query::OP_uc;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return { '#uc' => $node->{hoisted0}, '####need_function' => 1 };
}

package Foswiki::Query::OP_int;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return { '#int' => $node->{hoisted0}, '####need_function' => 1 };
}

#maths
package Foswiki::Query::OP_div;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return {
        '#div'               => [ $node->{hoisted0}, $node->{hoisted1} ],
        '####delay_function' => 15
    };
}

package Foswiki::Query::OP_minus;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return {
        '#minus'             => [ $node->{hoisted0}, $node->{hoisted1} ],
        '####delay_function' => 16
    };
}

package Foswiki::Query::OP_plus;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return {
        '#plus'              => [ $node->{hoisted0}, $node->{hoisted1} ],
        '####delay_function' => 17
    };
}

package Foswiki::Query::OP_times;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return {
        '#mult'              => [ $node->{hoisted0}, $node->{hoisted1} ],
        '####delay_function' => 18
    };
}

package Foswiki::Query::OP_neg;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return {
        '#neg'               => [ $node->{hoisted0}, $node->{hoisted1} ],
        '####delay_function' => 19,
        '####numeric'        => 1
    };
}

package Foswiki::Query::OP_pos;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return {
        '#pos'               => [ $node->{hoisted0}, $node->{hoisted1} ],
        '####delay_function' => 21,
        '####numeric'        => 1
    };
}

package Foswiki::Query::OP_empty;
use Assert;

#TODO: not sure this is the right answer..
sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    return {};
}

#array ops from versions.. (m3 work)
package Foswiki::Query::OP_comma;

#WARNING: this is now an nary op

package Foswiki::Query::OP_in;

######################################
package Foswiki::Query::OP_ref;
use Assert;

#use Data::Dumper;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;
    ASSERT( ref($node) eq 'Foswiki::Query::Node' );

    #print STDERR "---OP_ref(".Dumper($node).")\n";

    return {
        '####delay_function' => 22,
        '#ref'               => [ $node->{hoisted0}, $node->{hoisted1} ]
    };
}
######################################

=pod

mmm, cannot $not a $where

> db.current.find({_web: 'Sandbox',  '_topic' : 'AjaxComment'}, {_topic:1})    
{ "_id" : ObjectId("4d54110940f61a09b1a33186"), "_topic" : "AjaxComment" }
> 
> 
> db.current.find({_web: 'Sandbox',  $where: "this._topic == 'AjaxComment'"}, {_topic:1})
{ "_id" : ObjectId("4d54110940f61a09b1a33186"), "_topic" : "AjaxComment" }
> 
> db.current.find({_web: 'Sandbox',  $where: {$not : "this._topic == 'AjaxComment'"}}, {_topic:1})
error: { "$err" : "invalid use of $not", "code" : 13031 }
> 
> db.current.find({_web: 'Sandbox',  $not: {$where : "this._topic == 'AjaxComment'"}}, {_topic:1})  
error: { "$err" : "invalid operator: $where", "code" : 10068 }

-------------
mmm, can't $not a simple eq (have to unwind to work out to use $ne

> db.current.find({_web: 'Sandbox',  '_topic' : 'AjaxComment'}, {_topic:1})                       
{ "_id" : ObjectId("4d54110940f61a09b1a33186"), "_topic" : "AjaxComment" }
> db.current.find({_web: 'Sandbox',  '_topic' : {'$not':'AjaxComment'}}, {_topic:1})
error: { "$err" : "invalid use of $not", "code" : 13041 }

-------------------
but we can do very fun things with $in and $nin - including regexs

=cut

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
