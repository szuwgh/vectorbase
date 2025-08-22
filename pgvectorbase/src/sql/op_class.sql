CREATE OPERATOR CLASS l2_ops
	FOR TYPE vector USING hnsw_am AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops;