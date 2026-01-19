package main

import "math"

type QLNode struct {
	Type     uint32 // tagged union
	I64      int64
	Str      []byte
	Children []QLNode // operands
}

// statements: select, update, delete
type QLSelect struct {
	QLScan
	Names  []string // expr AS name
	Output []QLNode
}

type QLUpdate struct {
	QLScan
	Names  []string
	Values []QLNode
}

type QLDelete struct {
	QLScan
}

// common structure for statements: `INDEX BY`, `FILTER`, `LIMIT`
type QLScan struct {
	Table  string // table name
	Key1   QLNode // index by
	Key2   QLNode
	Filter QLNode // filter expression
	Offset int64  // limit
	Limit  int64
}

// =========================== Parser ===================

// Keep tracks of current parsing token / position
// Token are separated by ' ' or '\n'
type Parser struct {
	query []byte
	start int
	end   int
}

// Check for all keywords at start and
// advance to the next token (after keyword).
// Only advance if list match in that order.
func pKeyword(p *Parser, args ...string) bool {
	// TODO
	return true
}

// Check for any of the keyword at start,
// advance to the next token.
// Panic if cannot advance.
func pExpect(p *Parser, args ...string) {
	// TODO:
}

// Take the token at the current position as symbol and
// advance to the net token.
// Must be available, panic if not.
func pMustSym(p *Parser) string {
	// TODO
	return ""
}

// Parse statement into a structure
func pStmt(p *Parser) (r interface{}) {
	switch {
	case pKeyword(p, "create", "table"):
		r = pCreateTable(p)
	case pKeyword(p, "select"):
		r = pSelect(p)
		// ...
	}
	return r
}

// ================ Select ====================

func pSelect(p *Parser) *QLSelect {
	stmt := QLSelect{}
	pSelectExprList(p, &stmt) // SELECT xxx
	pExpect(p, "from", "expect `FROM` table")
	stmt.Table = pMustSym(p) // FROM table
	pScan(p, &stmt.QLScan)   // INDEX BY xxx FILTER yyy LIMIT zzz
	return &stmt
}

// Put the expression into the select node
func pSelectExpr(p *Parser, node *QLSelect) {
	st := pMustSym(p)
	node.Names = append(node.Names, st)
}

// SELECT a,b,c ...
func pSelectExprList(p *Parser, node *QLSelect) {
	pSelectExpr(p, node)
	for pKeyword(p, ",") {
		pSelectExpr(p, node)
	}
}

// ================= Scan ===================

func pScan(p *Parser, node *QLScan) {
	if pKeyword(p, "index", "by") {
		pIndexBy(p, node)
	}
	if pKeyword(p, "filter") {
		pExprOr(p, &node.Filter)
	}
	node.Offset, node.Limit = 0, math.MaxInt64
	if pKeyword(p, "limit") {
		pLimit(p, node)
	}
}

// =============== Expression ===================

// Take this token as a column
func pCol(p *Parser) QLNode {
	col := pMustSym(p)
	return QLNode{
		Type: 1, // Str
		Str:  []byte(col),
	}
}

// Put the add expression as node.
// Lower priority than mul
func pAdd(p *Parser) QLNode {
	node := pMul(p)
	// Keep finding the next '+'
	for next('+') {
		right := pMul(p)
		node = QLNode{
			Type:     '+',
			Children: []QLNode{node, right},
		}
	}
	return node
}

// Put the mul expression as node
func pMul(p *Parser) QLNode {
	node := pCol(p)
	// Keep finding the next '*'
	for next('*') {
		right := pCol(p)
		node = QLNode{
			Type:     '*',
			Children: []QLNode{node, right},
		}
	}
	return node
}

// ================================== Evaluator =======================

type QLEvalContex struct {
	rec Record // input row values
	out Value  // output
	err error
}

// Evaluate the expression on a record
func qlEval(ctx *QLEvalContex, node QLNode) {
	switch node.Type {
	// refer to a column
	case QL_SYM:
		if v := ctx.rec.Get(string(node.Str)); v != nil {
			ctx.out = *v
		} else {
			qlErr(ctx, "unknown column: %s", node.Str)
		}
	// a literal value
	case QL_I64, QL_STR:
		ctx.out = node.Value
	// operators
	case QL_NEG:
		qlEval(ctx, node.Children[0])
		if ctx.out.Type == TYPE_INT64 {
			ctx.out.I64 = -ctx.out.I64
		} else {
			qlErr(ctx, "QL_NEG type error")
		}
		// ...
	case QL_CMP_EQ:
		// ...
	}
}

func qlEvalScanKey(node QLNode) (Record, int, error) {
	// TODO
}

// Initiate a scanner with a QLScan.
// Scanner implement an INDEX BY condition
func qlScanInit(req *QLScan, sc *Scanner) (err error) {
	// convert `QLNode` to `Record` and `CMP_??`
	if sc.Key1, sc.Cmp1, err = qlEvalScanKey(req.Key1); err != nil {
		return err
	}
	if sc.Key2, sc.Cmp2, err = qlEvalScanKey(req.Key2); err != nil {
		return err
	}
	switch { // special handling when `Key1` and `Key2` are not both present
	case req.Key1.Type == 0 && req.Key2.Type == 0: // no `INDEX BY`
		sc.Cmp1, sc.Cmp2 = CMP_GE, CMP_LE // full table scan
	case req.Key1.Type == QL_CMP_EQ && req.Key2.Type == 0:
		// equal by a prefix: INDEX BY key = val
		sc.Key2 = sc.Key1
		sc.Cmp1, sc.Cmp2 = CMP_GE, CMP_LE
	case req.Key1.Type != 0 && req.Key2.Type == 0:
		// open-ended range: INDEX BY key > val
		if sc.Cmp1 > 0 {
			sc.Cmp2 = CMP_LE // compare with a zero-length tuple
		} else {
			sc.Cmp2 = CMP_GE
		}
	}
	return nil
}

// ================= Iterator =======================

type RecordIter interface {
	Valid() bool
	Next()
	Deref(*Record) error
}

// Get data from RecordIter, evaluate the expression in the select query back
type qlSelectIter struct {
	iter  RecordIter // input
	names []string
	exprs []QLNode
}

func (iter *qlSelectIter) Valid() bool {
	return iter.iter.Valid()
}
func (iter *qlSelectIter) Next() {
	iter.iter.Next()
}
func (iter *qlSelectIter) Deref(rec *Record) error {
	if err := iter.iter.Deref(rec); err != nil {
		return err
	}
	vals, err := qlEvalMulti(*rec, iter.exprs)
	if err != nil {
		return err
	}
	*rec = Record{iter.names, vals}
	return nil
}

// Iterate data with a QLScan query, underlying is a scanner
type qlScanIter struct {
	// input
	req *QLScan
	sc  Scanner
	// state
	idx int
	end bool
	// Cached state
	rec Record
}

func (iter *qlScanIter) Valid() bool {
	return iter.sc.Valid()
}

func (iter *qlScanIter) Next() {
	iter.sc.Next()
}

func isMatch(rec *Record, filter QLNode) bool {
	// Evaluate the expression
	ctx := QLEvalContex{
		rec: *rec,
		out: Value{},
		err: nil,
	}
	// TODO: Recursive evaluation
	qlEval(&ctx, filter)
	if ctx.err != nil {
		// Failed to evaluate this record with the expression,
		// maybe due to match fail. No match
		return false
	}
	return true
}

// Implement the FILTER condition
func (iter *qlScanIter) Deref(rec *Record) error {
	for {
		// Put temp result in self Record
		iter.sc.Deref(&iter.rec)
		// Check if it meets the filter
		if isMatch(&iter.rec, iter.req.Filter) {
			*rec = iter.rec
			break
		} else {
			iter.sc.Next()
			// TODO: Check end condition
		}
	}
	return nil
}
