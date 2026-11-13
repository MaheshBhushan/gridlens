from gridlens.views import view_statements


def test_view_statements_are_schema_qualified():
    stmts = view_statements("`workspace`.`gridlens`")
    assert len(stmts) == 5
    for s in stmts:
        assert s.startswith("CREATE OR REPLACE VIEW `workspace`.`gridlens`.v_")
        assert "{schema}" not in s
