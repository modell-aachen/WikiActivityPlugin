# See bottom of file for default license and copyright information

package Foswiki::Plugins::WikiActivityPlugin;

use strict;
use warnings;

use Foswiki::Func    ();
use Foswiki::Plugins ();

use DBI;
use Encode;
use JSON;

our $VERSION = '0.1';
our $RELEASE = '0.1';
our $SHORTDESCRIPTION = 'Tracks activity in the wiki and offers a query/presentation interface';
our $NO_PREFS_IN_TOPIC = 1;

my $db;
my %schema_versions;
my @schema_updates = (
    [
        # Basic relations
        "CREATE TABLE meta (type TEXT NOT NULL UNIQUE, version INT NOT NULL)",
        "INSERT INTO meta (type, version) VALUES('core', 0)",
        "CREATE TABLE subscriptions (
            id SERIAL PRIMARY KEY,
            base TEXT NOT NULL,
            user_id TEXT NOT NULL,
            sub_type TEXT NOT NULL,
            one_time BOOLEAN NOT NULL,
            read_before TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
            UNIQUE (base, user_id, sub_type)
        )",
        "CREATE INDEX subscriptions_base ON subscriptions (base)",
        "CREATE INDEX subscriptions_user_id ON subscriptions (user_id, read_before)",
        "CREATE TABLE events (
            id SERIAL PRIMARY KEY,
            base TEXT NOT NULL,
            topic TEXT,
            actor_id TEXT NOT NULL,
            verb TEXT NOT NULL,
            details jsonb,
            event_time TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
        )",
        "CREATE INDEX events_base ON events (base)",
        "CREATE INDEX events_actor_id ON events (actor_id)",
        "CREATE INDEX events_time ON events (event_time)",
    ],
);
sub initPlugin {
    my ( $topic, $web, $user, $installWeb ) = @_;

    if ( $Foswiki::Plugins::VERSION < 2.0 ) {
        Foswiki::Func::writeWarning( 'Version mismatch between ',
            __PACKAGE__, ' and Plugins.pm' );
        return 0;
    }

    #Foswiki::Func::registerTagHandler( 'EXAMPLETAG', \&_EXAMPLETAG );
    #Foswiki::Func::registerRESTHandler( 'example', \&restExample );

    return 1;
}

#sub _EXAMPLETAG {
#    my($session, $params, $topic, $web, $topicObject) = @_;
#}

sub finishPlugin {
    undef $db;
    undef %schema_versions;
}

sub db {
    return $db if defined $db;
    $db = DBI->connect($Foswiki::cfg{WikiActivityPlugin}{DSN},
        $Foswiki::cfg{WikiActivityPlugin}{DBUser},
        $Foswiki::cfg{WikiActivityPlugin}{DBPassword},
        {
            RaiseError => 1,
            PrintError => 0,
            AutoCommit => 1,
            FetchHashKeyName => 'NAME_lc',
        }
    );
    eval {
        %schema_versions = %{ $db->selectall_hashref("SELECT * FROM meta", 'type') };
    };
    _applySchema('core', @schema_updates);
    $db;
}
sub _applySchema {
    my $type = shift;
    if (!$schema_versions{$type}) {
        $schema_versions{$type} = { version => 0 };
    }
    my $v = $schema_versions{$type}{version};
    return if $v >= @_;
    for my $schema (@_[$v..$#_]) {
        $db->begin_work;
        for my $s (@$schema) {
            if (ref($s) eq 'CODE') {
                $s->($db);
            } else {
                $db->do($s);
            }
        }
        $db->do("UPDATE meta SET version=? WHERE type=?", {}, ++$v, $type);
        $db->commit;
    }
}
sub _insert {
    my ($table, $record) = @_;
    db()->do("INSERT INTO $table (". join(',', keys %$record) .") VALUES(". join(',', map { '?' } keys %$record) .")", {}, values %$record);
}

=begin TML

---++ StaticMethod addEvent( %opts )

Adds an event to the database. =%opts= contains the following keys:

   * =actor_id= (optional): the ID of the acting user; defaults to the current session's user.
   * =base= (required): the base object for this event, i.e. the origin topic. Other bases may exist depending on consumers of this API; they must make sure to not conflict with valid topic names. Example: 'news:15'
   * =topic= (optional): the concrete topic being affected. In a forking workflow, this might refer to the TALK topic being edited, while =base= refers to the workflow origin (the non-TALK topic).
   * =verb= (required): the action performed, as a textual ID. Examples: 'edit', 'approve'
   * =details= (optional): a JSON string containing arbitrary extra information about the event.

The return value is undefined. Exceptions may happen and should be handled by the caller.

=cut

sub addEvent {
    my %opts = @_;
    my %record;
    $record{actor_id} = $opts{user} || $Foswiki::Plugins::SESSION->{user};
    $record{base} = $opts{base};
    $record{topic} = $opts{topic} if exists $opts{topic};
    $record{verb} = $opts{verb};
    $record{details} = $opts{details} if exists $opts{details};
    _insert('events', \%record);
}

=begin TML

---++ StaticMethod addSubscription( %opts )

=cut

sub addSubscription {
    my %opts = @_;
    my %record;
    $record{user_id} = $opts{user} || $Foswiki::Plugins::SESSION->{user};
    $record{base} = $opts{base};
    $record{sub_type} = $opts{sub_type};
    $record{one_time} = $opts{one_time} if defined $opts{one_time};
    $record{read_before} = $opts{read_before} if defined $opts{read_before};
    # TODO: handle unique constraint violations
    _insert('subscriptions', \%record);
}


sub restUpdateSubscription {
   my ( $session, $subject, $verb, $response ) = @_;
   # TODO: timestamp etc.
}

1;

__END__
Foswiki - The Free and Open Source Wiki, http://foswiki.org/
WikiActivityPlugin extension

Author: %$AUTHOR%

Copyright (C) 2015 Modell Aachen GmbH

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version. For
more details read LICENSE in the root of the Foswiki
distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.
