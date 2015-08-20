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
my $json;
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
            one_time BOOLEAN NOT NULL DEFAULT 0,
            read_before TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
            UNIQUE (base, user_id, sub_type)
        )",
        "CREATE INDEX subscriptions_base ON subscriptions (base)",
        "CREATE INDEX subscriptions_user_id ON subscriptions (user_id, read_before)",
        "CREATE TABLE events (
            id SERIAL PRIMARY KEY,
            base TEXT NOT NULL,
            title TEXT NOT NULL,
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

    Foswiki::Func::registerRESTHandler( 'subscribe', \&restSubscribe,
        authenticate => 1,
        validate => 0,
        http_allow => 'POST',
    );
    Foswiki::Func::registerRESTHandler( 'unsubscribe', \&restUnsubscribe,
        authenticate => 1,
        validate => 0,
        http_allow => 'POST',
    );
    Foswiki::Func::registerRESTHandler( 'update_subscription', \&restUpdateSubscription,
        authenticate => 1,
        validate => 0,
        http_allow => 'POST',
    );
    Foswiki::Func::registerRESTHandler( 'subscribed_events_grouped', \&restSubscribedEventsGrouped,
        authenticate => 1,
        validate => 0,
        http_allow => 'POST,GET',
    );
    Foswiki::Func::registerRESTHandler( 'subscribed_events', \&restSubscribedEvents,
        authenticate => 1,
        validate => 0,
        http_allow => 'POST,GET',
    );
    Foswiki::Func::registerRESTHandler( 'subscribed_events_count', \&restSubscribedEventsCount,
        authenticate => 1,
        validate => 0,
        http_allow => 'POST,GET',
    );
    $json = JSON->new;
    return 1;
}

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
# Quick&dirty insert helper
sub _insert {
    my ($table, $record) = @_;
    db()->do("INSERT INTO $table (". join(',', keys %$record) .") VALUES(". join(',', map { '?' } keys %$record) .")", {}, values %$record);
}
# Mangle placeholders in SQL template
# Finds a named group: #foo{...} in the template and either keeps it (and adds
# variable binds to the existing list) or removes it
sub _sqlswitch {
    my ($group, $state, $sql, $params, @args) = @_;
    $_[2] =~ s!#$group\{(.*?)\}!
        $state ? ( push(@$params, @args), $1 ) : ''
    !eg;
}

=begin TML

---++ StaticMethod addEvent( %opts )

Adds an event to the database. =%opts= contains the following keys:

   * =user= (optional): the ID of the acting user; defaults to the current session's user.
   * =base= (required): the base object for this event, i.e. the origin topic. Other bases may exist depending on consumers of this API; they must make sure to not conflict with valid topic names. Example: 'news:15'
   * =title= (required): the title for this event or, more specifically, the base object this event relates to.
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
    $record{title} = $opts{title};
    $record{verb} = $opts{verb};
    $record{details} = $opts{details} if exists $opts{details};
    _insert('events', \%record);
}

=begin TML

---++ StaticMethod addSubscription( %opts )

Subscribes a user to an event base. =%opts= contains the following keys:

   * =user= (optional): the ID of the target user; defaults to the current session's user.
   * =base= (required): the base to subscribe to.
   * =sub_type= (required): type of subscription (to distinguish different ways of getting subscribed).
   * =one_time= (required): enable to automatically drop the subscription when the user marks it as read (not implemented yet).
   * =read_before= (optional): customize the time from which events on the base will be considered unread. Defaults to now.

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

=begin TML

---++ StaticMethod removeSubscription( %opts )

Unsubscribes a user from an event base. =%opts= contains the following keys:

   * =user= (optional): the ID of the target user; defaults to the current session's user.
   * =base= (required): the base to unsubscribe from.

=cut

sub removeSubscription {
    my %opts = @_;
    my $user = $opts{user} || $Foswiki::Plugins::SESSION->{user};
    my $base = $opts{base};
    db()->do("DELETE FROM subscriptions WHERE user=? AND base=?", {}, $user, $base);
}

# send JSON result
sub _writejson {
    my ($q, $resp, $data) = @_;
    $json->pretty(scalar $q->param('prettyjson'));
    $resp->header(-type => 'application/json');
    $resp->body($json->encode($data));
    return;
}

sub restSubscribedEventsGrouped {
    my ($session, $subject, $verb, $response) = @_;
    my $q = $session->{request};
    my $user = $session->{user};
    my $sql = "SELECT *, FLOOR(EXTRACT(EPOCH FROM e.event_time::timestamptz)) AS event_time, max(FLOOR(EXTRACT(EPOCH FROM event_time::timestamptz))) OVER pb AS maxtime, min(FLOOR(EXTRACT(EPOCH FROM event_time::timestamptz))) OVER pb AS mintime FROM events e WHERE base IN (SELECT base FROM events JOIN subscriptions USING (base) WHERE user_id = ?#unread{ AND event_time > read_before}#from{ AND event_time >= to_timestamp(?)}#to{ AND event_time <= to_timestamp(?)} GROUP BY base ORDER BY MAX(event_time) DESC LIMIT ? OFFSET ?)#outerunread{ AND event_time > (SELECT read_before FROM subscriptions WHERE user_id=? AND base=e.base)}#outerfrom{ AND event_time >= to_timestamp(?)}#outerto{ AND event_time <= to_timestamp(?)} WINDOW pb AS (PARTITION BY base) ORDER BY maxtime DESC, base, e.event_time DESC";
    my @args = ($user);
    _sqlswitch('unread', defined $q->param('all') ? !$q->param('all') : 1, $sql, \@args);
    _sqlswitch('from', defined $q->param('from'), $sql, \@args, $q->param('from'));
    _sqlswitch('to', defined $q->param('to'), $sql, \@args, $q->param('to'));
    push @args, ($q->param('count') || 20), ($q->param('offset') || 0);
    _sqlswitch('outerunread', defined $q->param('all') ? !$q->param('all') : 1, $sql, \@args, $user);
    _sqlswitch('outerfrom', defined $q->param('outerfrom'), $sql, \@args, $q->param('outerfrom'));
    _sqlswitch('outerto', defined $q->param('outerto'), $sql, \@args, $q->param('outerto'));
    my $events = db()->selectall_arrayref($sql, {Slice => {}}, @args);
    my $grouped_events = [];
    my @bases;
    my %buckets;
    for my $e (@$events) {
        my $base = $e->{base};
        if (!$buckets{$base}) {
            push @bases, $base;
            $buckets{$base} = [];
        }
        push @{$buckets{$base}}, $e;
    }
    for my $b (keys %buckets) {
        push @$grouped_events, {
            base => $b,
            mintime => $buckets{$b}[0]{mintime},
            maxtime => $buckets{$b}[0]{maxtime},
            title => $buckets{$b}[0]{title},
            events => $buckets{$b},
        };
    }
    _writejson($q, $response, {
        status => 'success',
        data => $grouped_events,
    });
}

sub restSubscribedEvents {
    my ($session, $subject, $verb, $response) = @_;
    my $q = $session->{request};

    my $offset = $q->param('offset') || 0;
    my $count = $q->param('count') || 10;

    my $sql = "SELECT DISTINCT e.*, FLOOR(EXTRACT(EPOCH FROM e.event_time::timestamptz)) AS event_time from events e JOIN subscriptions s USING (base) WHERE s.user_id = ?#unread{ AND e.event_time > s.read_before}#from{ AND e.event_time >= to_timestamp(?)}#to{ AND e.event_time <= to_timestamp(?)} ORDER BY e.event_time DESC LIMIT ? OFFSET ?";
    my @args = ($session->{user});
    _sqlswitch('unread', defined $q->param('all') ? !$q->param('all') : 1, $sql, \@args);
    _sqlswitch('from', defined $q->param('from'), $sql, \@args, $q->param('from'));
    _sqlswitch('to', defined $q->param('to'), $sql, \@args, $q->param('to'));
    push @args, $count, $offset;

    my $res = db()->selectall_arrayref($sql, {Slice => {}}, @args);
    _writejson($q, $response, {
        status => 'success',
        data => $res,
    });
}

sub restSubscribedEventsCount {
    my ($session, $subject, $verb, $response) = @_;
    my $q = $session->{request};

    my $sql = "SELECT COUNT(e.id) AS total_events, COUNT(DISTINCT e.base) AS event_bases FROM events e JOIN subscriptions s USING (base) WHERE s.user_id=?#unread{ AND e.event_time > s.read_before}#from{ AND e.event_time >= to_timestamp(?)}#to{ AND e.event_time <= to_timestamp(?)}";
    my @args = $session->{user};
    _sqlswitch('unread', defined $q->param('all') ? !$q->param('all') : 1, $sql, \@args);
    _sqlswitch('from', defined $q->param('from'), $sql, \@args, $q->param('from'));
    _sqlswitch('to', defined $q->param('to'), $sql, \@args, $q->param('to'));
    _writejson($q, $response, {
        status => 'success',
        data => db()->selectrow_hashref($sql, {}, @args)
    });
}

sub restSubscribe {
    my ($session, $subject, $verb, $response) = @_;
    my $q = $session->{request};
    my $base = $q->param('base');
    addSubscription(
        base => $base,
        sub_type => 'subscription',
    );
    _writejson({status => 'success'});
}
sub restUnsubscribe {
    my ($session, $subject, $verb, $response) = @_;
    my $q = $session->{request};
    my $user = $session->{user};
    my $base = $q->param('base');
    db()->do("DELETE FROM subscriptions WHERE base=? AND user_id=?", {},
        $base, $user
    );
    _writejson({status => 'success'});
}
sub restUpdateSubscription {
    my ($session, $subject, $verb, $response) = @_;
    my $q = $session->{request};
    my $user = $session->{user};
    my $base = $q->param('base');
    my $ts = $q->param('ts') || time;
    # Rewind timestamp: useful for testing or reviewing past events
    $ts += time if $ts =~ /^-/;

    my $sql = "UPDATE subscriptions SET read_before=to_timestamp(?) WHERE user_id=?#base{ AND base=?}";
    my @args = ($ts, $user);
    _sqlswitch('base', defined $base, $sql, \@args, $base);
    db()->do($sql, {}, @args);
    _writejson({status => 'success'});
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
