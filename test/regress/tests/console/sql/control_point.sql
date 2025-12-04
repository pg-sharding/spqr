-- Test ATTACH CONTROL POINT command
ATTACH CONTROL POINT '2pc_decision_cp';

-- Test DETACH CONTROL POINT command
DETACH CONTROL POINT '2pc_decision_cp';

-- Test error case: unknown control point
ATTACH CONTROL POINT 'unknown_cp';
DETACH CONTROL POINT 'unknown_cp';
