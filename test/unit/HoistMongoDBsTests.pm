# Test for hoisting REs from query expressions
package HoistMongoDBsTests;

use FoswikiFnTestCase;
our @ISA = qw( FoswikiFnTestCase );

use Foswiki::Query::Parser;
use Foswiki::Plugins::MongoDBPlugin::HoistMongoDB;
use Foswiki::Query::Node;
use Foswiki::Meta;
use Data::Dumper;
use strict;

use constant MONITOR => 0;

#list of operators we can output
my @MongoOperators = qw/$or $not $nin $in/;
#list of all Query ops
#TODO: build this from code?
my @QueryOps = qw/== != > < =~ ~/;

#TODO: use the above to test operator coverage - fail until we have full coverage.
#TODO: test must run _last_


sub do_Assert {
    my $this                 = shift;
    my $s                = shift;
    my $expectedMongoDBQuery = shift;
    my $expectedSimplifiedMongoDBQuery = shift;
    
    print STDERR "SEARCH: $s\n" if MONITOR;
    
    #non-simplfied query
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);

    #    print STDERR "HoistS ",$query->stringify();
    print STDERR "HoistS ", Dumper($query) if MONITOR;

    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    print STDERR "\n -> /", Dumper($mongoDBQuery), "/\n" if MONITOR;

    $this->assert_deep_equals( $expectedMongoDBQuery, $mongoDBQuery );
    
   #try out converttoJavascript
   print STDERR "\nconvertToJavascript: \n".Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript($mongoDBQuery)."\n" if MONITOR;
   
   $this->do_SimplifiedAssert($s, $expectedSimplifiedMongoDBQuery || $expectedMongoDBQuery);
}

sub do_SimplifiedAssert {
    my $this                 = shift;
    my $s                = shift;
    my $expectedMongoDBQuery = shift;
    
    print STDERR "SEARCH: $s\n" if MONITOR;
    
    #non-simplfied query
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);

    print STDERR "HoistS ",$query->stringify()."\n" if MONITOR;
    print STDERR "HoistS ", Dumper($query) if MONITOR;

    print STDERR "StringHoistS ",$query->stringify()."\n" if MONITOR;
    my $context = Foswiki::Meta->new( $this->{session}, $this->{session}->{webName} );
    $query->simplify( tom => $context, data => $context );
    print STDERR "PosterHoistS ",$query->stringify()."\n" if MONITOR;
    
#    if  ( $query->evaluatesToConstant() ) {
    if (1==2){
        #not an interesting hoist.
        #should test for true/false..
        print STDERR "simplified to a constant..\n";
        #not sure howto test this atm
    } else {
            print STDERR "SimplifiedHoistS ", Dumper($query) if MONITOR;

            my $mongoDBQuery =
              Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

            print STDERR "\nsimplified ->  ", Dumper($mongoDBQuery), "/\n" if MONITOR;
            print STDERR "\nexpected ->  ", Dumper($expectedMongoDBQuery), "/\n" if MONITOR;
            $this->assert_deep_equals( $expectedMongoDBQuery, $mongoDBQuery );
    }

   #try out converttoJavascript
   #print STDERR "\nconvertToJavascript: \n".Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript($mongoDBQuery)."\n" if MONITOR;
}

sub set_up {
    my $this = shift;
    $this->SUPER::set_up();

    my $meta = Foswiki::Meta->new( $this->{session}, 'Web', 'Topic' );
    $meta->putKeyed(
        'FILEATTACHMENT',
        {
            name    => "att1.dat",
            attr    => "H",
            comment => "Wun",
            path    => 'a path',
            size    => '1',
            user    => 'Junkie',
            rev     => '23',
            date    => '25',
        }
    );
    $meta->putKeyed(
        'FILEATTACHMENT',
        {
            name    => "att2.dot",
            attr    => "",
            comment => "Too",
            path    => 'anuvver path',
            size    => '100',
            user    => 'ProjectContributor',
            rev     => '105',
            date    => '99',
        }
    );
    $meta->put( 'FORM', { name => 'TestForm' } );
    $meta->put(
        'TOPICINFO',
        {
            author  => 'AlbertCamus',
            date    => '12345',
            format  => '1.1',
            version => '1.1913',
        }
    );
    $meta->put(
        'TOPICMOVED',
        {
            by   => 'AlbertCamus',
            date => '54321',
            from => 'BouvardEtPecuchet',
            to   => 'ThePlague',
        }
    );
    $meta->put( 'TOPICPARENT', { name => '' } );
    $meta->putKeyed( 'PREFERENCE', { name => 'Red',    value => '0' } );
    $meta->putKeyed( 'PREFERENCE', { name => 'Green',  value => '1' } );
    $meta->putKeyed( 'PREFERENCE', { name => 'Blue',   value => '0' } );
    $meta->putKeyed( 'PREFERENCE', { name => 'White',  value => '0' } );
    $meta->putKeyed( 'PREFERENCE', { name => 'Yellow', value => '1' } );
    $meta->putKeyed( 'FIELD',
        { name => "number", title => "Number", value => "99" } );
    $meta->putKeyed( 'FIELD',
        { name => "string", title => "String", value => "String" } );
    $meta->putKeyed(
        'FIELD',
        {
            name  => "StringWithChars",
            title => "StringWithChars",
            value => "n\nn t\tt s\\s q'q o#o h#h X~X \\b \\a \\e \\f \\r \\cX"
        }
    );
    $meta->putKeyed( 'FIELD',
        { name => "boolean", title => "Boolean", value => "1" } );
    $meta->putKeyed( 'FIELD', { name => "macro", value => "%RED%" } );

    $meta->{_text} = "Green ideas sleep furiously";

    $this->{meta} = $meta;
}

sub test_hoistSimple {
    my $this        = shift;
    my $s           = "number=99";
    $this->do_Assert( $s,  { 'FIELD.number.value' => '99' } );
}

sub test_hoistSimple_OP_Like {
    my $this        = shift;
    my $s           = "String~'.*rin.*'";

    $this->do_Assert( $s, 
        { 'FIELD.String.value' => qr/(?-xism:\..*rin\..*)/ } );
}

sub test_hoistSimple2 {
    my $this        = shift;
    my $s           = "99=number";


#TODO: should really reverse these, but it is harder with strings - (i think the lhs in  'web.topic'/something is a string..
    $this->do_Assert( $s,  { '99' => 'FIELD.number.value' } );
}

sub test_hoistOR {
    my $this        = shift;
    my $s           = "number=12 or string='bana'";


    $this->do_Assert(
        $s,
        {
            '$or' => [
                { 'FIELD.number.value' => '12' },
                { 'FIELD.string.value' => 'bana' }
            ]
        }
    );
}

sub test_hoistOROR {
    my $this        = shift;
    my $s           = "number=12 or string='bana' or string = 'apple'";


    $this->do_Assert(
        $s,

        {
            '$or' => [
                { 'FIELD.number.value' => '12' },
                { 'FIELD.string.value' => 'bana' },
                { 'FIELD.string.value' => 'apple' }
            ]
        }
    );
}

sub test_hoistBraceOROR {
    my $this        = shift;
    my $s           = "(number=12 or string='bana' or string = 'apple')";

    $this->do_Assert(
        $s,
        {
            '$or' => [
                { 'FIELD.number.value' => '12' },
                { 'FIELD.string.value' => 'bana' },
                { 'FIELD.string.value' => 'apple' }
            ]
        }
    );
}

sub test_hoistANDBraceOROR {
    my $this = shift;
    my $s =
      "(number=12 or string='bana' or string = 'apple') AND (something=12)";


    $this->do_Assert(
        $s,

        {
            'FIELD.something.value' => '12',
            '$or'                   => [
                { 'FIELD.number.value' => '12' },
                { 'FIELD.string.value' => 'bana' },
                { 'FIELD.string.value' => 'apple' }
            ]
        }
    );
}


sub test_hoistBraceANDBrace_OPTIMISE {
    my $this = shift;
    my $s    = "(TargetRelease = 'minor') AND (TargetRelease = 'minor')";


    $this->do_Assert(
        $s,

        {
            'FIELD.TargetRelease.value' => 'minor' 
        }
    );
}

#need to optimise it, as mongo cna't have 2 keys of the same name, its queries are a hash
sub test_hoistBraceANDBrace {
    my $this = shift;
    my $s    = "(TargetRelease != 'minor') AND (TargetRelease != 'major')";


    $this->do_Assert(
        $s,

        {
          'FIELD.TargetRelease.value' => {
                                           '$nin' => [
                                                       'minor',
                                                       'major'
                                                     ]
                                         }
        }
    );
}

sub test_hoistBrace {
    my $this        = shift;
    my $s           = "(number=12)";


    $this->do_Assert( $s, { 'FIELD.number.value' => '12' } );
}

sub test_hoistAND {
    my $this        = shift;
    my $s           = "number=12 and string='bana'";


    $this->do_Assert(
        $s,

        {
            'FIELD.number.value' => '12',
            'FIELD.string.value' => 'bana'
        }
    );
}

sub test_hoistANDAND {
    my $this        = shift;
    my $s           = "number=12 and string='bana' and something='nothing'";


    $this->do_Assert(
        $s,

        {
            'FIELD.number.value'    => '12',
            'FIELD.something.value' => 'nothing',
            'FIELD.string.value'    => 'bana'
        }
    );
}

sub test_hoistSimpleFieldDOT {
    my $this        = shift;
    my $s           = "FIELD.number.bana = 12";


    #TODO: there's and assumption that the bit before the . is the form-name
    $this->do_Assert( $s, { 'FIELD.bana.value' => '12' } );
}
sub test_hoistMETAFieldDOT {
    my $this        = shift;
    my $s           = "META:FIELD.number.bana = 12";


    #TODO: there's and assumption that the bit before the . is the form-name
    $this->do_Assert( $s, { 'FIELD.bana.value' => '12' } );
}

sub test_hoistSimpleDOT {
    my $this        = shift;
    my $s           = "number.bana = 12";


    #TODO: there's and assumption that the bit before the . is the form-name
    $this->do_Assert( $s, { 'FIELD.bana.value' => '12' } );
}
sub test_hoistSimpleField {
    my $this        = shift;
    my $s           = "number = 12";


    #TODO: there's and assumption that the bit before the . is the form-name
    $this->do_Assert( $s, { 'FIELD.number.value' => '12' } );
}

sub test_hoistGT {
    my $this        = shift;
    my $s           = "number>12";


    $this->do_Assert( $s,
        { 'FIELD.number.value' => { '$gt' => '12' } } );
}

sub test_hoistGTE {
    my $this        = shift;
    my $s           = "number>=12";


    $this->do_Assert( $s,
        { 'FIELD.number.value' => { '$gte' => '12' } } );
}

sub test_hoistLT {
    my $this        = shift;
    my $s           = "number<12";


    $this->do_Assert( $s,
        { 'FIELD.number.value' => { '$lt' => '12' } } );
}

sub test_hoistLTE {
    my $this        = shift;
    my $s           = "number<=12";


    $this->do_Assert( $s,
        { 'FIELD.number.value' => { '$lte' => '12' } } );
}

sub test_hoistEQ {
    my $this        = shift;
    my $s           = "number=12";


    $this->do_Assert( $s, { 'FIELD.number.value' => '12' } );
}

sub test_hoistNE {
    my $this        = shift;
    my $s           = "number!=12";


    $this->do_Assert( $s,
        { 'FIELD.number.value' => { '$ne' => '12' } } );
}

sub test_hoistNOT_EQ {
    my $this        = shift;
    my $s           = "not(number=12)";

    $this->do_Assert( $s,
        { 'FIELD.number.value' => { '$ne' => '12' } } );
}

sub test_hoistCompound {
    my $this = shift;
    my $s =
"number=99 AND string='String' and (moved.by='AlbertCamus' OR moved.by ~ '*bert*')";

    $this->do_Assert(
        $s,

        {
            'FIELD.number.value' => '99',
            '$or'                => [
                { 'TOPICMOVED.by' => 'AlbertCamus' },
                { 'TOPICMOVED.by' => qr/(?-xism:.*bert.*)/ }
            ],
            'FIELD.string.value' => 'String'
        }
    );
}

sub test_hoistCompound2 {
    my $this = shift;
    my $s =
"(moved.by='AlbertCamus' OR moved.by ~ '*bert*') AND number=99 AND string='String'";


    $this->do_Assert(
        $s,

        {
            'FIELD.number.value' => '99',
            'FIELD.string.value' => 'String',
            '$or'                => [
                { 'TOPICMOVED.by' => 'AlbertCamus' },
                { 'TOPICMOVED.by' => qr/(?-xism:.*bert.*)/ }
            ]
        }
    );
}

sub test_hoistAlias {
    my $this        = shift;
    my $s           = "info.date=12345";


    $this->do_Assert( $s, { 'TOPICINFO.date' => '12345' } );
}

sub test_hoistFormField {
    my $this        = shift;
    my $s           = "TestForm.number=99";


    $this->do_Assert( $s, { 'FIELD.number.value' => '99' } );
}

sub test_hoistText {
    my $this        = shift;
    my $s           = "text ~ '*Green*'";

    $this->do_Assert( $s,
        { '_text' => qr/(?-xism:.*Green.*)/ } );
}

sub test_hoistName {
    my $this        = shift;
    my $s           = "name ~ 'Web*'";


    $this->do_Assert( $s,
        { '_topic' => qr/(?-xism:Web.*)/ } );
}

sub test_hoistName2 {
    my $this        = shift;
    my $s           = "name ~ 'Web*' OR name ~ 'A*' OR name = 'Banana'";


    $this->do_Assert(
        $s,

        {
            '$or' => [
                { '_topic' => qr/(?-xism:Web.*)/ },
                { '_topic' => qr/(?-xism:A.*)/ },
                { '_topic' => 'Banana' }
            ]
        }
    );
}

sub test_hoistOP_Match {
    my $this        = shift;
    my $s           = "text =~ '.*Green.*'";


    $this->do_Assert( $s,
        { '_text' => qr/(?-xism:.*Green.*)/ } );
}


sub test_hoistOP_Where {
    my $this        = shift;
    my $s           = "preferences[name='SVEN']";


# db.current.find({ 'PREFERENCE.__RAW_ARRAY' : { '$elemMatch' : {'name' : 'SVEN' }}})

    $this->do_Assert( $s,
        { 'PREFERENCE.__RAW_ARRAY' => { '$elemMatch' => {'name' => 'SVEN' }}}
        );
}
#
sub test_hoistOP_Where1 {
    my $this        = shift;
    my $s           = "fields[value='FrequentlyAskedQuestion']";


# db.current.find({ 'PREFERENCE.__RAW_ARRAY' : { '$elemMatch' : {'name' : 'SVEN' }}})

    $this->do_Assert( $s,
        {
          'FIELD.__RAW_ARRAY' => {
                                                    '$elemMatch' => {
                                                                      'value' => 'FrequentlyAskedQuestion'
                                                                    }
                                                  }
                                              }
        );
}
sub test_hoistOP_Where2 {
    my $this        = shift;
    my $s           = "META:FIELD[value='FrequentlyAskedQuestion']";


# db.current.find({ 'PREFERENCE.__RAW_ARRAY' : { '$elemMatch' : {'name' : 'SVEN' }}})

    $this->do_Assert( $s,
        {
          'FIELD.__RAW_ARRAY' => {
                                                    '$elemMatch' => {
                                                                      'value' => 'FrequentlyAskedQuestion'
                                                                    }
                                                  }
                                              }
        );
}
sub test_hoistOP_Where3 {
    my $this        = shift;
    my $s           = "META:FIELD[name='TopicClassification' AND value='FrequentlyAskedQuestion']";


# db.current.find({ 'PREFERENCE.__RAW_ARRAY' : { '$elemMatch' : {'name' : 'SVEN' }}})

    $this->do_Assert( $s,
    {
          'FIELD.__RAW_ARRAY' => {
                                   '$elemMatch' => {
                                                     'value' => 'FrequentlyAskedQuestion',
                                                     'name' => 'TopicClassification'
                                                   }
                                 }
        }
        );
}
sub test_hoistOP_Where4 {
    my $this        = shift;
    my $s           = "META:FIELD[name='TopicClassification'][value='FrequentlyAskedQuestion']";


# db.current.find({ 'PREFERENCE.__RAW_ARRAY' : { '$elemMatch' : {'name' : 'SVEN' }}})

    $this->do_Assert( $s,
    {
          'FIELD.__RAW_ARRAY' => {
                                   '$elemMatch' => {
                                                     'value' => 'FrequentlyAskedQuestion',
                                                     'name' => 'TopicClassification'
                                                   }
                                 }
        }
        );
}
#i think this is meaninless, but i'm not sure.
sub test_hoistOP_preferencesDotName {
    my $this        = shift;
    my $s           = "preferences.name='BLAH'";


    $this->do_Assert( $s,
        {  'PREFERENCE.name' => 'BLAH' } );
}

sub test_hoistORANDOR {
    my $this        = shift;
    my $s           = "(number=14 OR number=12) and (string='apple' OR string='bana')";


    $this->do_Assert(
        $s,

{
          'FIELD.number.value' => {
                                    '$in' => [
                                               '14',
                                               '12'
                                             ]
                                  },
          'FIELD.string.value' => {
                                    '$in' => [
                                               'apple',
                                               'bana'
                                             ]
                                  }
        }
    );
}

sub test_hoistLcRHSName {
    my $this        = shift;
    my $s           = "name = lc('WebHome')";


    $this->do_Assert( $s,
        {
            '$where' => "this._topic == foswiki_toLowerCase('WebHome')"
        },
        {
              '_topic' => 'webhome'
        }
        );
}


sub test_hoistLcLHSField {
    my $this        = shift;
    my $s           = "lc(Subject) = 'WebHome'";


    $this->do_Assert( $s,
        {
            '$where' => "foswiki_toLowerCase(foswiki_getField(this, 'FIELD.Subject.value')) == 'WebHome'"
        }
        );
}

sub test_hoistLcLHSName {
    my $this        = shift;
    my $s           = "lc(name) = 'WebHome'";


    $this->do_Assert( $s,
        {
            '$where' => "foswiki_toLowerCase(this._topic) == 'WebHome'"
        }
        );
}

sub DISABLEtest_hoistLcRHSLikeName {
#TODO: this requires the hoister to notice that its a constant and that it can pre-evaluate it
    my $this        = shift;
    my $s           = "name ~ lc('Web*')";


    $this->do_Assert( $s,
        { '_topic' => qr/(?-xism:web.*)/ } );
}


sub test_hoistLcLHSLikeName {
    my $this        = shift;
    my $s           = "lc(name) ~ 'Web*'";


    $this->do_Assert( $s,
        {
            '$where' => "( /^Web.*\$/.test(foswiki_toLowerCase(this._topic)) )"
        }
        );
}

sub test_hoistLengthLHSName {
    my $this        = shift;
    my $s           = "length(name) = 12";


    $this->do_Assert( $s,
        {
            '$where' => "foswiki_length(this._topic) == 12"
        }
        );
}
sub test_hoistLengthLHSString {
    my $this        = shift;
    my $s           = "length('something') = 9";


    $this->do_Assert( $s,
        {
            '$where' => "foswiki_length('something') == 9"
        },
        {
        }
        );
}
sub test_hoistLengthLHSString_false {
    my $this        = shift;
    my $s           = "length('FALSEsomething') = 9";


    $this->do_Assert( $s,
        {
            '$where' => "foswiki_length('FALSEsomething') == 9"
        },
        {
            '1' => '0'
        }
        );
}

sub test_hoistLengthLHSNameGT {
    my $this        = shift;
    my $s           = "length(name) < 12";


    $this->do_Assert( $s,
        {
            '$where' => "foswiki_length(this._topic) < 12"
        }
        );
}

sub test_hoist_d2n_value {
    my $this        = shift;
    my $s           = "d2n noatime";


    $this->do_Assert( $s,
        {
          '$where' => "foswiki_d2n(foswiki_getField(this, 'FIELD.noatime.value'))"
        }
        );
}

sub test_hoist_d2n_valueAND {
    my $this        = shift;
    my $s           = "d2n(noatime) and topic='WebHome'";


    $this->do_Assert( $s,
        {
#TODO: need to figure out how to not make both into js
          '$where' => " ( (foswiki_d2n(foswiki_getField(this, 'FIELD.noatime.value'))) )  && foswiki_getField(this, 'FIELD.topic.value') == 'WebHome'"
#          '$where' => 'foswiki_d2n(this.FIELD.noatime.value)',
#          'FIELD.topic.value' => 'WebHome'
        }
        );
}

sub test_hoist_d2n {
    my $this        = shift;
    my $s           = "d2n(name) < d2n('1998-11-23')";


    $this->do_Assert( $s,
        {
           '$where' => "foswiki_d2n(this._topic) < foswiki_d2n('1998-11-23')"
        },
        {
           '$where' => "foswiki_d2n(this._topic) < 911743200"
        }
        );
}


sub test_hoist_Item10323_1 {
    my $this        = shift;
    my $s           = "lc(TermGroup)=~'bio'";


    $this->do_Assert( $s,
        {
               '$where' => "( /bio/.test(foswiki_toLowerCase(foswiki_getField(this, 'FIELD.TermGroup.value'))) )"
        }
        );
}
sub test_hoist_Item10323_2 {
    my $this        = shift;
    my $s           = "lc(TermGroup)=~lc('bio')";


    $this->do_Assert( $s,
        {
#           '$where' => " (Regex('bio'.toLowerCase(), '').test(this.FIELD.TermGroup.value.toLowerCase())) "
#find a special case
            'FIELD.TermGroup.value' => qr/(?i-xsm:.*bio.*)/i
        },
        {
                      '$where' => '( /bio/.test(foswiki_toLowerCase(foswiki_getField(this, \'FIELD.TermGroup.value\'))) )'
        }

        );
}

sub test_hoist_Item10323_2_not {
    my $this        = shift;
    my $s           = "not(lc(TermGroup)=~lc('bio'))";


    $this->do_Assert( $s,
        {
#          '$where' => "! (  ( (Regex('bio'.toLowerCase(), '').test(this.FIELD.TermGroup.value.toLowerCase())) )  ) "
          'FIELD.TermGroup.value' => {
                                       '$not' => qr/(?i-xsm:bio)/
                                     }

        },
        {
                      '$where' => '! ( /bio/.test(foswiki_toLowerCase(foswiki_getField(this, \'FIELD.TermGroup.value\'))) )'
        }
        );
}

sub test_hoist_Item10323 {
    my $this        = shift;
    my $s           = "form.name~'*TermForm' AND lc(Namespace)=~lc('ant') AND lc(TermGroup)=~lc('bio')";


    $this->do_Assert( $s,
        {
          'FORM.name' => qr/(?-xism:^.*TermForm$)/,
#          '$where' => ' ( (Regex(\'ant\'.toLowerCase(), \'\').test(this.FIELD.Namespace.value.toLowerCase())) )  &&  ( (Regex(\'bio\'.toLowerCase(), \'\').test(this.FIELD.TermGroup.value.toLowerCase())) ) '
          'FIELD.TermGroup.value' => qr/(?i-xsm:bio)/,
          'FIELD.Namespace.value' => qr/(?i-xsm:ant)/
        },
        {
            #TODO: why is this js?
                 '$where' => ' ( ( (( /^.*TermForm$/.test(foswiki_getField(this, \'FORM.name\')) )) )  &&  (( /ant/.test(foswiki_toLowerCase(foswiki_getField(this, \'FIELD.Namespace.value\'))) )) )  &&  (( /bio/.test(foswiki_toLowerCase(foswiki_getField(this, \'FIELD.TermGroup.value\'))) )) ' 
        }
        );
}

sub test_hoist_maths {
    my $this        = shift;
    my $s           = "(12-Namespace)<(24*60*60-5) AND (TermGroup DIV 12)>(WebScale*42.8)";


    $this->do_Assert( $s,
        {
           '$where' =>  " ( ((12)-(foswiki_getField(this, 'FIELD.Namespace.value')) < (((24)*(60))*(60))-(5)) )  &&  ((foswiki_getField(this, 'FIELD.TermGroup.value'))/(12) > (foswiki_getField(this, 'FIELD.WebScale.value'))*(42.8)) "
        },
        {
           '$where' =>  " ( ((12)-(foswiki_getField(this, 'FIELD.Namespace.value')) < 86395) )  &&  ((foswiki_getField(this, 'FIELD.TermGroup.value'))/(12) > (foswiki_getField(this, 'FIELD.WebScale.value'))*(42.8)) "
        }        );
}
sub test_hoist_concat {
    my $this        = shift;
    my $s           = "'asd' + 'qwe' = 'asdqwe'";


    $this->do_Assert( $s,
        {
           '$where' => '(\'asd\')+(\'qwe\') == \'asdqwe\''
        },
        {
        }
        );
}
#this one is a nasty perler-ism
sub test_hoist_concat2 {
    my $this        = shift;
    my $s           = "'2' + '3' = '5'";


    $this->do_Assert( $s,
        {
           '$where' => '(2)+(3) == 5'
        },
        {}
        );
}
sub test_hoist_concat3 {
    my $this        = shift;
    my $s           = "2 + 3 = 5";


    $this->do_Assert( $s,
        {
           '$where' => '(2)+(3) == 5'
        },
        {}
        );
}
sub test_hoist_concat_false {
    my $this        = shift;
    my $s           = "'FALSEasd' + 'qwe' = 'asdqwe'";


    $this->do_Assert( $s,
        {
           '$where' => '(\'FALSEasd\')+(\'qwe\') == \'asdqwe\''
        },
        {
            '1' => '0'
        }
        );
}
#this one is a nasty perler-ism
sub test_hoist_concat2_false {
    my $this        = shift;
    my $s           = "'9' + '3' = '5'";


    $this->do_Assert( $s,
        {
           '$where' => '(9)+(3) == 5'
        },
        {
                        '1' => '0'
        }
        );
}
sub test_hoist_concat3_false {
    my $this        = shift;
    my $s           = "9 + 3 = 5";


    $this->do_Assert( $s,
        {
           '$where' => '(9)+(3) == 5'
        },
        {            '1' => '0'
        }
        );
}

sub UNTRUE_test_hoist_shorthandPref {
    my $this        = shift;
    my $s           = "Red=12";


    $this->do_Assert( $s,
        {
                     'PREFERENCE.__RAW_ARRAY' => {
                                        '$elemMatch' => {
                                                          'value' => '12',
                                                          'name' => 'Red'
                                                        }
                                      }
        }
        );
}
sub test_hoist_longhandPref {
    my $this        = shift;
    my $s           = "preferences[value=12].Red";


    $this->do_Assert( $s,
        {
                     'PREFERENCE.__RAW_ARRAY' => {
                                        '$elemMatch' => {
                                                          'value' => '12',
                                                          'name' => 'Red'
                                                        }
                                      }
        }
        );
}
sub test_hoist_longhandField_value {
    my $this        = shift;
#see QueryTests::verify_meta_squabs_MongoDBQuery
    my $s           = "fields[name='number'].value";


    $this->do_Assert( $s,
        {
                     'FIELD.__RAW_ARRAY' => {
                                        '$elemMatch' => {
                                                          'name' => 'number'
                                                        }
                                      }
        }
        );
}


sub test_hoist_longhand2Pref {
    my $this        = shift;
    my $s           = "preferences[value=12 AND name='Red']";


    $this->do_Assert( $s,
        {
                     'PREFERENCE.__RAW_ARRAY' => {
                                        '$elemMatch' => {
                                                          'value' => '12',
                                                          'name' => 'Red'
                                                        }
                                      }
        }
        );
}

sub BROKENtest_hoist_PrefPlusAccessor {
    my $this        = shift;
    my $s           = "preferences[value=12].name = 'Red'";


    $this->do_Assert( $s,
        {
                     'PREFERENCE.__RAW_ARRAY' => {
                                        '$elemMatch' => {
                                                          'value' => '12',
                                                          'name' => 'Red'
                                                        }
                                      }
        }
        );
}


#this is basically a SEARCH with both the topic= and excludetopic= set
sub test_hoistTopicNameIncludeANDNOExclude {
    my $this = shift;
    my $s =
      "name='Item' AND (something=12 or something=999 or something=123)";


    $this->do_Assert(
        $s,

        {
          '$or' => [
                     {
                       'FIELD.something.value' => '12'
                     },
                     {
                       'FIELD.something.value' => '999'
                     },
                     {
                       'FIELD.something.value' => '123'
                     }
                   ],
          '_topic' => 'Item'
            }
    );
}

sub test_hoistTopicNameNOIncludeANDExclude {
    my $this = shift;
    my $s =
      "(NOT(name='Item')) AND (something=12 or something=999 or something=123)";


    $this->do_Assert(
        $s,

        {
          '$or' => [
                     {
                       'FIELD.something.value' => '12'
                     },
                     {
                       'FIELD.something.value' => '999'
                     },
                     {
                       'FIELD.something.value' => '123'
                     }
                   ],
          '_topic' => { '$ne'=>'Item' }
            }
    );
}

sub test_hoistTopicNameNOIncludeANDExclude2 {
    my $this = shift;
    my $s =
      "((name!='Item')) AND (something=12 or something=999 or something=123)";


    $this->do_Assert(
        $s,

        {
          '$or' => [
                     {
                       'FIELD.something.value' => '12'
                     },
                     {
                       'FIELD.something.value' => '999'
                     },
                     {
                       'FIELD.something.value' => '123'
                     }
                   ],
          '_topic' => { '$ne'=>'Item' }
            }
    );
}

sub test_hoistTopicNameIncludeANDExclude {
    my $this = shift;
    my $s =
      "(name='Item' AND NOT name='ItemTemplate') AND (something=12 or something=999 or something=123)";


    $this->do_Assert(
        $s,

        {
          '$or' => [
                     {
                       'FIELD.something.value' => '12'
                     },
                     {
                       'FIELD.something.value' => '999'
                     },
                     {
                       'FIELD.something.value' => '123'
                     }
                   ],
#          '$where' => '! ( this._topic == \'ItemTemplate\' ) ',
          '_topic' => {
                        '$nin' => [
                                    'ItemTemplate'
                                  ],
                        '$in' => [
                                   'Item'
                                 ]
                      }
            }
    );
}

sub test_hoistTopicNameIncludeRegANDExclude {
    my $this = shift;
    my $s =
      "(name~'Item*' AND NOT name='ItemTemplate') AND (something=12 or something=999 or something=123)";


    $this->do_Assert(
        $s,

        {
          '$or' => [
                     {
                       'FIELD.something.value' => '12'
                     },
                     {
                       'FIELD.something.value' => '999'
                     },
                     {
                       'FIELD.something.value' => '123'
                     }
                   ],
#          '$where' => '! ( this._topic == \'ItemTemplate\' ) ',
          '_topic' => {
                        '$nin' => [
                                    'ItemTemplate'
                                  ],
                        '$in' => [
                                   qr/(?-xism:^Item.*$)/
                                 ]
                      }
            }
    );
}
sub test_hoistTopicNameIncludeRegANDExcludeReg {
    my $this = shift;
    my $s =
      "(name~'Item*' AND NOT name~'*Template') AND (something=12 or something=999 or something=123)";


    $this->do_Assert(
        $s,

        {
          '$or' => [
                     {
                       'FIELD.something.value' => '12'
                     },
                     {
                       'FIELD.something.value' => '999'
                     },
                     {
                       'FIELD.something.value' => '123'
                     }
                   ],
#          '$where' => '! ( ( /^.*Template$/.test(this._topic) ) ) ',
          '_topic' => {
                        '$nin' => [
                                    qr/(?-xism:^.*Template$)/
                                  ],
                        '$in' => [
                                   qr/(?-xism:^Item.*$)/
                                 ]
                      }
            }
    );
}

sub test_hoist_dateAndRelationship {
    my $this = shift;
    my $s = "form.name~'*RelationshipForm' AND ( (NOW - info.date) < (60*60*24*7))";


    $this->do_Assert(
        $s,

        {
#TODO: this is caused by the delay_function at line 360 of the hoister ('why')
#          'FORM.name' => qr/(?-xism:^.*RelationshipForm$)/,
#          '$where' => "(foswiki_getField(this, 'FIELD.NOW.value'))-(foswiki_getField(this, 'TOPICINFO.date')) < (((60)*(60))*(24))*(7)"
            '$where' => ' ( (( /^.*RelationshipForm$/.test(foswiki_getField(this, \'FORM.name\')) )) )  &&  ((foswiki_getField(this, \'FIELD.NOW.value\'))-(foswiki_getField(this, \'TOPICINFO.date\')) < (((60)*(60))*(24))*(7)) '
        },
        {
            '$where' => ' ( (( /^.*RelationshipForm$/.test(foswiki_getField(this, \'FORM.name\')) )) )  &&  ((foswiki_getField(this, \'FIELD.NOW.value\'))-(foswiki_getField(this, \'TOPICINFO.date\')) < 604800) '
        }
    );
}

sub test_hoist_MultiAnd {
    my $this = shift;
#ignore that this could be optimised out - we're testing that the hoister manages to get the logic reasonalbe'
    my $s = "(name = 'fest' AND name = 'test' AND name = 'pest')";


    $this->do_Assert(
        $s,

        {
          '$where' => ' (this._topic == \'test\')  && this._topic == \'pest\'',
          '_topic' => 'fest'
        }
    );
#   $this->assert_equals(
#            " ( (this._topic == 'test')  && this._topic == 'pest')  && this._topic == 'fest'", 
 #           Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript($mongoDBQuery)
 #           );
}

sub test_hoist_not_in {
#this tests a number of things, including that the 'not' actually goes around all the inner logic
    my $this = shift;
    my $s = "not(name = 'fest' AND name = 'test' AND name = 'pest')";


    $this->do_Assert(
        $s,

        {
          '$where' => '! (  ( (this._topic == \'test\')  && this._topic == \'pest\')  && this._topic == \'fest\' ) '
#            '$where' => {
#                '$ne' => " ( (this._topic == 'test')  && this._topic == 'pest')  && this._topic == 'fest'"
#            }
        }
    );
#   $this->assert_equals(
#            '! (  ( (this._topic == \'test\')  && this._topic == \'pest\')  && this._topic == \'fest\' ) ',
#            Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript($mongoDBQuery)
#            );
}

sub test_hoist_not_in2 {
    my $this = shift;
    my $s = "not(name = 'fest') AND not(name = 'test') AND not(name = 'pest')";
#    my $s = "name != 'fest' AND name != 'test' AND name != 'pest'";


    $this->do_Assert(
        $s,

        {
#          '$where' => ' ( (! ( this._topic == \'fest\' ) )  &&  (! ( this._topic == \'test\' ) ) )  &&  (! ( this._topic == \'pest\' ) ) '
#OR
#          '_topic' => {
#                        '$not' => {
#                                    '$in' => [
#                                               'fest',
#                                               'test',
#                                               'pest'
#                                             ]
#                                  }
#                      }
#OR
#TODO: the OP_and gets confused sometimes
          '$where' => 'this._topic != \'pest\'',
          '_topic' => {
                        '$nin' => [
                                    'fest',
                                    'test'
                                  ]
                      }

        }
    );
#   $this->assert_equals(
##            " ( (this._topic ! == 'fest' || this._topic ! == 'test' || this._topic ! == 'pest' ) ) ", 
##            " ( ( (! ( this._topic == \'fest\' ) )  &&  (! ( this._topic == \'test\' ) ) )  &&  (! ( this._topic == \'pest\' ) ) ) ",
#            " (this._topic != 'pest')  && ! ( this._topic == 'fest' || this._topic == 'test' ) ",
#            Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript($mongoDBQuery)
#            );
}

sub test_hoist_ref {
    my $this = shift;
    my $s = "'AnotherTopic'/number = 12";


    $this->do_Assert(
        $s,

        {
          '$where' => "(foswiki_getField(foswiki_getRef(\'localhost\', foswiki_getDatabaseName(this._web), \'current\', this._web, 'AnotherTopic'), 'FIELD.number.value')) == 12"
        },
        {
            '1' => '0'
        }
    );
}

sub test_hoist_ref2 {
    my $this = shift;
    my $s = "Source/info.rev!=SourceRev";
#    my $s = "form.name='TaxonProfile.Relationships.RelationshipForm' AND Source/info.rev!=SourceRev";


    $this->do_Assert(
        $s,

        {
          '$where' => '(foswiki_getField(foswiki_getRef(\'localhost\', foswiki_getDatabaseName(this._web), \'current\', this._web, foswiki_getField(this, \'FIELD.Source.value\')), \'TOPICINFO.rev\')) != foswiki_getField(this, \'FIELD.SourceRev.value\')'

        }
    );
}
sub test_hoist_ref3 {
    my $this = shift;
    my $s = "SourceRev>Source/info.rev";
#    my $s = "form.name='TaxonProfile.Relationships.RelationshipForm' AND Source/info.rev!=SourceRev";


    $this->do_Assert(
        $s,

        {
          '$where' => 'foswiki_getField(this, \'FIELD.SourceRev.value\') > (foswiki_getField(foswiki_getRef(\'localhost\', foswiki_getDatabaseName(this._web), \'current\', this._web, foswiki_getField(this, \'FIELD.Source.value\')), \'TOPICINFO.rev\'))'
        }
    );
}
sub test_hoist_ref4 {
    my $this = shift;
    my $s = "form.name='TaxonProfile.Relationships.RelationshipForm' AND Source/info.rev!=SourceRev";


    $this->do_Assert(
        $s,

        {
          '$where' => ' ( (foswiki_getField(this, \'FORM.name\') == \'TaxonProfile.Relationships.RelationshipForm\') )  &&  ((foswiki_getField(foswiki_getRef(\'localhost\', foswiki_getDatabaseName(this._web), \'current\', this._web, foswiki_getField(this, \'FIELD.Source.value\')), \'TOPICINFO.rev\')) != foswiki_getField(this, \'FIELD.SourceRev.value\')) '
        }
    );
}
sub test_hoist_ref4_or {
    my $this = shift;
    my $s = "form.name='TaxonProfile.Relationships.RelationshipForm' OR Source/info.rev!=SourceRev";


    $this->do_Assert(
        $s,

        {
          '$where' => ' ( foswiki_getField(this, \'FORM.name\') == \'TaxonProfile.Relationships.RelationshipForm\' || (foswiki_getField(foswiki_getRef(\'localhost\', foswiki_getDatabaseName(this._web), \'current\', this._web, foswiki_getField(this, \'FIELD.Source.value\')), \'TOPICINFO.rev\')) != foswiki_getField(this, \'FIELD.SourceRev.value\') ) '
        }
    );
}
sub test_hoist_ref4_longhand {
    my $this = shift;
    my $s = "META:FORM.name='TaxonProfile.Relationships.RelationshipForm' AND Source/META:TOPICINFO.rev!=SourceRev";


    $this->do_Assert(
        $s,

        {
          '$where' => ' ( (foswiki_getField(this, \'FORM.name\') == \'TaxonProfile.Relationships.RelationshipForm\') )  &&  ((foswiki_getField(foswiki_getRef(\'localhost\', foswiki_getDatabaseName(this._web), \'current\', this._web, foswiki_getField(this, \'FIELD.Source.value\')), \'TOPICINFO.rev\')) != foswiki_getField(this, \'FIELD.SourceRev.value\')) '
        }
    );
}
sub test_hoist_parent {
    my $this = shift;
    my $s = "parent.name='WebHome'";


    $this->do_Assert(
        $s,

        {
            'TOPICPARENT.name' => 'WebHome'
        }
    );
}
sub test_hoist_parent_longhand {
    my $this = shift;
    my $s = "META:TOPICPARENT.name='WebHome'";


    $this->do_Assert(
        $s,

        {
            'TOPICPARENT.name' => 'WebHome'
        }
    );
}

sub test_hoist_Item10515 {
    my $this = shift;
    my $s = "lc(Firstname)=lc('JOHN')";


    $this->do_Assert(
        $s,

        {
          '$where' => 'foswiki_toLowerCase(foswiki_getField(this, \'FIELD.Firstname.value\')) == foswiki_toLowerCase(\'JOHN\')'
        },
        {
          '$where' => 'foswiki_toLowerCase(foswiki_getField(this, \'FIELD.Firstname.value\')) == \'john\''
        }
    );
}

sub test_hoist_false {
    my $this = shift;
    my $s = "0";


    $this->do_Assert(
        $s,

        {
            #TODO: this is not really a true query in mongo, and just happens to return nothing :/
            '1' => '0'
        }
    );
}
sub test_hoist_explicit_false {
    my $this = shift;
    my $s = "'0'";


    $this->do_Assert(
        $s,

        {
            #TODO: this is not really a true query in mongo, and just happens to return nothing :/
            '1' => '0'
        }
    );
}

sub test_hoist_true {
    my $this = shift;
    my $s = "1";


    $this->do_Assert(
        $s,

        {
        }
    );
}
sub test_hoist_explicit_true {
    my $this = shift;
    my $s = "'1'";


    $this->do_Assert(
        $s,

        {
        }
    );
}

#test written to match Fn_SEARCH::verify_formQuery2
#Item10520: in Sven's reading of System.QuerySearch, this should return no results, as there is no field of the name 'TestForm'
sub DISABLEtest_hoist_ImplicitFormNameBUG {
    my $this = shift;
    my $s = "FormName";


    $this->do_Assert(
        $s,

        {
          '$where' => '(foswiki_getField(this, \'FIELD.FormName.name\') )'
        }
    );
}

sub test_hoist_ref_TOPICINFO_longhand {
    my $this = shift;
    my $s = "'Main.WebHome'/META:TOPICINFO.date";


    $this->do_Assert(
        $s,
        {
            '$where' => '(foswiki_getField(foswiki_getRef(\'localhost\', foswiki_getDatabaseName(this._web), \'current\', this._web, \'Main.WebHome\'), \'TOPICINFO.date\'))'
        },
        {
        '$where' => 1231502400
        }
    );
}
sub test_hoist_ref_TOPICINFO_longhand_plus_WEBHome {
    my $this = shift;
    my $s = "(not (name = 'AnotherTopic' or name = 'WebHome' or name = 'BarnicalBob')) and 'Main.WebChanges'/META:TOPICINFO.date";


    $this->do_Assert(
        $s,

        {
            '$where' => " ( ((! ( this._topic == 'AnotherTopic' || this._topic == 'WebHome' || this._topic == 'BarnicalBob' ) )) )  &&  ((foswiki_getField(foswiki_getRef('localhost', foswiki_getDatabaseName(this._web), 'current', this._web, 'Main.WebChanges'), 'TOPICINFO.date'))) "
        },
        {
          '$where' => 1231502400,
          '$nor' => [
                      {
                        '_topic' => 'AnotherTopic'
                      },
                      {
                        '_topic' => 'WebHome'
                      },
                      {
                        '_topic' => 'BarnicalBob'
                      }
                    ]

        }
    );
}

sub test_hoist_ref_TOPICINFO_longhand_plus {
    my $this = shift;
    my $s = "(not (name = 'AnotherTopic' or name = 'WebHome' or name = 'BarnicalBob')) and 'AnotherTopic'/META:TOPICINFO.date";

    #just to make sure that this topic actually does not exist.
    $this->assert(not Foswiki::Func::topicExists($this->{test_web}, 'AnotherTopic'));

    $this->do_Assert(
        $s,

        {
            '$where' => " ( ((! ( this._topic == 'AnotherTopic' || this._topic == 'WebHome' || this._topic == 'BarnicalBob' ) )) )  &&  ((foswiki_getField(foswiki_getRef('localhost', foswiki_getDatabaseName(this._web), 'current', this._web, 'AnotherTopic'), 'TOPICINFO.date'))) "
        },
        {
          '1' => '0',   #our false
          '$nor' => [
                      {
                        '_topic' => 'AnotherTopic'
                      },
                      {
                        '_topic' => 'WebHome'
                      },
                      {
                        '_topic' => 'BarnicalBob'
                      }
                    ]
        }
    );
}
sub test_hoist_CREATEINFO_longhand {
    my $this = shift;
    my $s = "META:CREATEINFO.date > 12346787";


    $this->do_Assert(
        $s,

        {
            'CREATEINFO.date' => {'$gt' => 12346787 }
        }
    );
}

sub test_hoist_ref_CREATEINFO_longhand {
    my $this = shift;
    my $s = "'AnotherTopic'/META:CREATEINFO.date";


    $this->do_Assert(
        $s,

        {
            '$where' => '(foswiki_getField(foswiki_getRef(\'localhost\', foswiki_getDatabaseName(this._web), \'current\', this._web, \'AnotherTopic\'), \'CREATEINFO.date\'))'
        },
        #that topic does not exist, so we're false.
        { '$where' => '0'
        }
    );
}

1;
