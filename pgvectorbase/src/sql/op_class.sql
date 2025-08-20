CREATE OPERATOR CLASS l2_ops
	FOR TYPE tensor USING hnsw_am AS
	OPERATOR 1 <-> (tensor, tensor) FOR ORDER BY float_ops;