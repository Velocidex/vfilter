package reformat

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/Velocidex/ordereddict"
	"github.com/alecthomas/assert"
	"github.com/sebdah/goldie/v2"
	"www.velocidex.com/golang/vfilter"
	"www.velocidex.com/golang/vfilter/types"
)

type reformatTestCase struct {
	name, vql string
}

var reformatTests = []reformatTestCase{
	{"MultiLine Comment", `
/*
   This is a multiline comment
*/
SELECT Foo FROM plugin()
`},

	{"Comment on columns", `
SELECT Foo,
       -- This is a comment on Bar
       Bar
FROM plugin()
`},

	{"Comment in a comma list", `
    LET LocalDeviceMajor <= (
       253,
       7,   -- loop
       8,   -- sd
    )
`},

	{"Comment above plugin arg", `
SELECT * FROM info(
     -- This is a comment for Foo
     Foo = 1,
 Bar =2
)
`},

	{"Comment above SELECT", `
// This is a comment
-- Another type of comment
SELECT * FROM info()
`},
	{"Comma Expression", `select * from plugin() where dict(foo=[1,])`},
	{"Comma Expression 2", `select * from plugin() where (1, 2, 3, 4)`},
	{"Group by with many columns", `SELECT min(item=EventTime) AS EarliestTime
FROM source(artifact="Exchange.Windows.Detection.ISOMount") GROUP BY Computer, Username, EventID, Filename`},
	{"Plugin with long first arg", `SELECT *
FROM Artifact.Windows.EventLogs.EvtxHunter(EvtxGlob='''%SystemRoot%\System32\Winevt\Logs\*Sysmon*.evtx''')`},
	{"If Plugin", `LET DateAfterTime <= if(condition=TargetTime, then=timestamp(epoch=TargetTime.Unix - TargetTimeBox), else=timestamp(epoch=now() - TargetTimeBox))`},
	{"If Plugin with longer args", `LET DateAfterTime <= if(condition=TargetTime, then=timestamp(epoch=TargetTime.Unix - TargetTimeBox + SomeConstant), else=timestamp(epoch=now() - TargetTimeBox))`},
	{"Try to fit plugin args in one line", `LET CacheList = SELECT FullPath FROM glob(globs=GlobPath, root=RootPath)`},
	{"If plugin args break lines line up on (", `LET CacheList = SELECT FullPath FROM glob(globs=GlobPath, follow_symlinks=TRUE, another_long_arg=54,  root="C:/Windows/System32/")`},
	{"If plugin args are too long to line up on ( they get indent more", `LET CacheList = SELECT FullPath FROM glob(globs=Glob, follow_symlinks=TRUE, another_long_arg=54,  root="C:/Windows/System32/Foobar/Config/Registry/Some/Long/Path/And/Along/Path")`},

	{"Try to keep function args in one line", `LET CacheList = SELECT FullPath FROM glob(globs=split(string=CacheGlob, sep=","), root=RootPath)`},
	{"Example", `LET CacheList = SELECT FullPath FROM glob(globs=Glob, root=RootPath)`},
	{"LET expression", "LET Foo = SELECT * FROM info() WHERE Foo GROUP BY 1"},
	{"LET with parameters", "LET Foo(x,y,z) = SELECT * FROM info() WHERE Foo GROUP BY 1"},
	{"Nesting", "SELECT * FROM foreach(row={SELECT * FROM foreach(row={SELECT * FROM info}, query={SELECT * FROM info()})}, query=ForeachQuery(X=1))"},
	{"Foreach", "SELECT * FROM foreach(row={SELECT * FROM clients() LIMIT 5}, query={SELECT client_info(client_id=client_id) FROM glob(globs=AGlob)})"},
	{"Subquery", "SELECT {SELECT * FROM info()} AS Foo, Bar FROM scope()"},
	{"Simple Statement", "SELECT A AS First, B AS Second, C, D FROM info(arg=1, arg2=3) WHERE 1 ORDER BY C LIMIT 1"},
}

func makeTestScope() types.Scope {
	env := ordereddict.NewDict()
	result := vfilter.NewScope().AppendVars(env)
	result.SetLogger(log.New(os.Stdout, "Log: ", log.Ldate|log.Ltime|log.Lshortfile))
	return result
}

func TestVQLQueries(t *testing.T) {
	// Store the result in ordered dict so we have a consistent golden file.
	scope := makeTestScope()
	golden := ""

	for _, testCase := range reformatTests {
		vql, err := ReFormatVQL(
			scope, testCase.vql, vfilter.DefaultFormatOptions)
		assert.NoError(t, err)

		golden += fmt.Sprintf("%v:\n%v\n\n", testCase.name, vql)
	}

	g := goldie.New(
		t,
		goldie.WithFixtureDir("fixtures"),
		goldie.WithNameSuffix(".golden"),
		goldie.WithDiffEngine(goldie.ColoredDiff),
	)
	g.Assert(t, "formatting", []byte(golden))
}
