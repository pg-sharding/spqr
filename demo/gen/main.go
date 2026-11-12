// Command gen writes the SPQR messenger demo script to stdout:
//
//	go run ./demo/gen -size s > demo/messenger.sql
//
// The output depends only on -size, so every build of a size is identical.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"time"
)

type size struct {
	chats, users, messages uint64
}

var sizes = map[string]size{
	"s": {1_000, 500, 100_000},
	"m": {100_000, 50_000, 10_000_000},
	"l": {1_000_000, 500_000, 100_000_000},
}

const (
	direct  = "direct"
	group   = "group"
	channel = "channel"

	tsLayout = "2006-01-02 15:04:05+00"
	epoch    = 1704067200 // 2024-01-01 00:00:00 UTC
	year     = 365 * 24 * 3600
)

var (
	languages = [][2]string{
		{"en", "English"}, {"es", "Spanish"}, {"de", "German"}, {"fr", "French"},
		{"pt", "Portuguese"}, {"ru", "Russian"}, {"sr", "Serbian"}, {"tr", "Turkish"},
	}
	emoji = [][2]string{
		{":+1:", "thumbs up"}, {":heart:", "red heart"}, {":joy:", "face with tears of joy"},
		{":fire:", "fire"}, {":tada:", "party popper"}, {":eyes:", "eyes"},
		{":thinking:", "thinking face"}, {":cry:", "crying face"},
	}
	firstNames = []string{
		"Alice", "Boris", "Chen", "Dana", "Emre", "Fatima", "Goran", "Hana", "Ivan", "Julia",
		"Kofi", "Lena", "Marko", "Nina", "Omar", "Petra", "Quinn", "Rosa", "Sven", "Tara",
	}
	lastNames = []string{
		"Adams", "Becker", "Costa", "Dimitrov", "Evans", "Fischer", "Garcia", "Horvat", "Ivanov", "Jovanovic",
		"Kaya", "Lopez", "Meyer", "Novak", "Okafor", "Petrov", "Rossi", "Silva", "Tanaka", "Weber",
	}
	adjectives = []string{
		"Weekend", "Morning", "Secret", "Friendly", "Remote", "Local", "Late Night", "Sunday",
		"Lazy", "Brave", "Quiet", "Loud",
	}
	nouns = []string{
		"Hikers", "Gamers", "Cooks", "Runners", "Readers", "Neighbours", "Cyclists", "Builders",
		"Gardeners", "Travellers", "Musicians", "Climbers",
	}
	topics = []string{
		"Postgres", "Sharding", "Go", "Databases", "Startups", "Football", "Movies", "Space",
		"Coffee", "Music", "Travel", "Weather",
	}
	openers = []string{
		"Hey,", "Ok,", "Well,", "Guys,", "By the way,", "Honestly,", "Quick question:", "Reminder:",
		"Update:", "So,", "Listen,", "Morning!", "Wow,", "Sorry,", "Thanks,", "Hmm,",
	}
	closers = []string{
		"are we still on for tonight?", "I will be ten minutes late.", "did anyone see the game?",
		"the build is green again.", "lunch at noon?", "check the link I sent.", "that was a great talk.",
		"who is bringing the snacks?", "the table is booked for eight.", "I pushed the fix.",
		"call me when you are free.", "see you tomorrow.", "happy birthday!", "it is raining again.",
		"the meeting is moved to Friday.", "can you review my patch?", "we need one more shard.",
		"I am on my way.", "this is fine.", "let's vote.", "photos are in the album.",
		"the train is delayed.", "good night everyone.", "no idea, ask Ivan.",
	}
)

type rng struct{ *rand.PCG }

func newRng(stream, id uint64) rng { return rng{rand.NewPCG(stream, id)} }

func (r rng) n(n uint64) uint64 { return r.Uint64() % n }

func pick(r rng, s []string) string { return s[r.n(uint64(len(s)))] }

func ts(unix uint64) string { return time.Unix(int64(unix), 0).UTC().Format(tsLayout) }

type chat struct {
	kind    string
	created uint64
	members uint64
	first   uint64
}

type gen struct {
	size
	name   string
	counts []uint32
	w      *bufio.Writer
}

func (g *gen) chat(id uint64) chat {
	r := newRng(1, id)
	c := chat{created: epoch + r.n(year), first: r.n(g.users)}
	switch p := r.n(100); {
	case id <= g.chats/100 || p >= 95:
		c.kind, c.members = channel, 20+r.n(81)
	case p < 60:
		c.kind, c.members = direct, 2
	default:
		c.kind, c.members = group, 3+r.n(18)
	}
	return c
}

func (g *gen) member(c chat, i uint64) uint64 { return (c.first+i)%g.users + 1 }

func firstName(user uint64) string { return pick(newRng(0, user), firstNames) }

func (g *gen) title(id uint64, c chat) string {
	r := newRng(4, id)
	switch c.kind {
	case direct:
		return firstName(g.member(c, 0)) + " and " + firstName(g.member(c, 1))
	case group:
		return pick(r, adjectives) + " " + pick(r, nouns)
	default:
		return pick(r, topics) + " Daily"
	}
}

// messageCounts spreads the messages over chats with weight 1/(chat_id + chats/100),
// so the low end of the key space holds most of the data: the first half of the
// chats gets about 85% of the messages.
func messageCounts(s size) []uint32 {
	weight := func(id uint64) uint64 { return (1 << 32) / (id + s.chats/100) }
	var total uint64
	for id := uint64(1); id <= s.chats; id++ {
		total += weight(id)
	}
	counts := make([]uint32, s.chats+1)
	var cum, prev uint64
	for id := uint64(1); id <= s.chats; id++ {
		cum += weight(id)
		upTo := cum * s.messages / total
		counts[id] = uint32(upTo - prev)
		prev = upTo
	}
	return counts
}

func (g *gen) printf(format string, a ...any) {
	if _, err := fmt.Fprintf(g.w, format, a...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func (g *gen) copy(table, columns string, rows func()) {
	g.printf("COPY %s (%s) FROM STDIN;\n", table, columns)
	rows()
	g.printf("\\.\n\n")
}

func (g *gen) rules() {
	g.printf(`-- SPQR demo database: messenger, size %[1]s.
-- Generated by demo/gen, do not edit.
--
--   psql "host=<router> port=6432 dbname=<db> user=<user>" -f messenger.sql
--
-- Needs an empty SPQR cluster with two or more shards and a user with admin
-- rights. The admin console is expected on port 7432 of the same host; pass
-- -v admin_port=<port> to change it. psql does not reuse the password after a
-- port change, so set PGPASSWORD or use ~/.pgpass.
--
-- To load again, run drop.sql first.

\set ON_ERROR_STOP on
\if :{?admin_port}
\else
\set admin_port 7432
\endif
\set router_port :PORT

\connect - - - :admin_port

CREATE DISTRIBUTION IF NOT EXISTS ds_chats COLUMN TYPES integer;
CREATE KEY RANGES FOR DISTRIBUTION ds_chats BETWEEN 1 AND %[2]d;
ALTER DISTRIBUTION ds_chats ATTACH RELATION IF NOT EXISTS chats DISTRIBUTION KEY chat_id;
ALTER DISTRIBUTION ds_chats ATTACH RELATION IF NOT EXISTS chat_members DISTRIBUTION KEY chat_id;
ALTER DISTRIBUTION ds_chats ATTACH RELATION IF NOT EXISTS "messages" DISTRIBUTION KEY chat_id;
ALTER DISTRIBUTION ds_chats ATTACH RELATION IF NOT EXISTS reactions DISTRIBUTION KEY chat_id;
CREATE REFERENCE TABLE IF NOT EXISTS users;
CREATE REFERENCE TABLE IF NOT EXISTS emoji;
CREATE REFERENCE TABLE IF NOT EXISTS languages;

\connect - - - :router_port

`, g.name, g.chats+1)
}

func (g *gen) schema() {
	g.printf(`CREATE TABLE IF NOT EXISTS languages (
    language_code text PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE IF NOT EXISTS emoji (
    emoji text PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    user_id bigint PRIMARY KEY,
    name text NOT NULL,
    language_code text NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS chats (
    chat_id bigint PRIMARY KEY,
    title text NOT NULL,
    kind text NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS chat_members (
    chat_id bigint NOT NULL,
    user_id bigint NOT NULL,
    role text NOT NULL,
    joined_at timestamptz NOT NULL,
    PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS messages (
    chat_id bigint NOT NULL,
    message_id bigint NOT NULL,
    author_id bigint NOT NULL,
    body text NOT NULL,
    sent_at timestamptz NOT NULL,
    PRIMARY KEY (chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS reactions (
    chat_id bigint NOT NULL,
    message_id bigint NOT NULL,
    user_id bigint NOT NULL,
    emoji text NOT NULL,
    PRIMARY KEY (chat_id, message_id, user_id)
);

`)
}

func (g *gen) data() {
	g.copy("languages", "language_code, name", func() {
		for _, l := range languages {
			g.printf("%s\t%s\n", l[0], l[1])
		}
	})
	g.copy("emoji", "emoji, name", func() {
		for _, e := range emoji {
			g.printf("%s\t%s\n", e[0], e[1])
		}
	})
	g.copy("users", "user_id, name, language_code, created_at", func() {
		for id := uint64(1); id <= g.users; id++ {
			r := newRng(5, id)
			g.printf("%d\t%s %s\t%s\t%s\n", id, firstName(id), pick(r, lastNames),
				languages[r.n(uint64(len(languages)))][0], ts(epoch-1-r.n(year)))
		}
	})
	g.copy("chats", "chat_id, title, kind, created_at", func() {
		for id := uint64(1); id <= g.chats; id++ {
			c := g.chat(id)
			g.printf("%d\t%s\t%s\t%s\n", id, g.title(id, c), c.kind, ts(c.created))
		}
	})
	g.copy("chat_members", "chat_id, user_id, role, joined_at", func() {
		for id := uint64(1); id <= g.chats; id++ {
			c := g.chat(id)
			for i := range c.members {
				role := "member"
				if c.kind != direct && i == 0 {
					role = "owner"
				}
				g.printf("%d\t%d\t%s\t%s\n", id, g.member(c, i), role, ts(c.created+60*i))
			}
		}
	})
	g.copy("messages", "chat_id, message_id, author_id, body, sent_at", func() {
		for id := uint64(1); id <= g.chats; id++ {
			c := g.chat(id)
			r := newRng(2, id)
			authors := c.members
			if c.kind == channel {
				authors = 3
			}
			sent := c.created
			for m := uint64(1); m <= uint64(g.counts[id]); m++ {
				sent += 1 + r.n(600)
				g.printf("%d\t%d\t%d\t%s %s\t%s\n", id, m, g.member(c, r.n(authors)),
					pick(r, openers), pick(r, closers), ts(sent))
			}
		}
	})
	g.copy("reactions", "chat_id, message_id, user_id, emoji", func() {
		for id := uint64(1); id <= g.chats; id++ {
			c := g.chat(id)
			r := newRng(3, id)
			for m := uint64(1); m <= uint64(g.counts[id]); m++ {
				if r.n(5) != 0 {
					continue
				}
				first := r.n(c.members)
				for i := range min(1+r.n(3), c.members) {
					g.printf("%d\t%d\t%d\t%s\n", id, m, g.member(c, (first+i)%c.members),
						emoji[r.n(uint64(len(emoji)))][0])
				}
			}
		}
	})
}

func (g *gen) summary() {
	g.printf(`\set ON_ERROR_STOP off
SET __spqr__reply_notice TO true;

\echo
\echo Messenger demo is loaded. Rows on each shard, in the order of the NOTICE:
SELECT count(*) AS chats FROM chats /* __spqr__scatter_query: true */;
SELECT count(*) AS messages FROM messages /* __spqr__scatter_query: true */;

\echo Try:
\echo '  SELECT * FROM messages WHERE chat_id = 1 ORDER BY message_id DESC LIMIT 5;'
\echo '  SELECT * FROM messages WHERE chat_id = %d ORDER BY message_id DESC LIMIT 5;'
\echo '  SELECT chat_id FROM chat_members WHERE user_id = 42 /* __spqr__scatter_query: true */;'
`, g.chats)
}

func main() {
	name := flag.String("size", "s", "dataset size: s, m or l")
	flag.Parse()
	s, ok := sizes[*name]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown size %q\n", *name)
		os.Exit(2)
	}

	g := &gen{size: s, name: *name, counts: messageCounts(s), w: bufio.NewWriterSize(os.Stdout, 1<<20)}
	g.rules()
	g.schema()
	g.data()
	g.summary()
	if err := g.w.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
