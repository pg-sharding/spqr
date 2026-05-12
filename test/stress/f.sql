BEGIN;
SET __spqr__engine_v2 TO true;
SET __spqr__linearize_dispatch TO true;

INSERT INTO t (i, j) VALUES (1, 0), (200, 0), (300, 0) ON CONFLICT (i) DO UPDATE SET j = t.j + 1 /*__spqr__linearize_dispatch: ok */;

COMMIT;

-- with force auto-linearize
BEGIN;
SET __spqr__engine_v2 TO true;
SET __spqr__linearize_dispatch TO false;

INSERT INTO t (i, j) VALUES (1, 0), (200, 0), (300, 0) ON CONFLICT (i) DO UPDATE SET j = t.j + 1 /*__spqr__linearize_dispatch: ok */;

COMMIT;
