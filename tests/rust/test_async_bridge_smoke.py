def test_rustawaitable_class_is_exported():
    from django_cachex._driver import RustAwaitable

    assert RustAwaitable.__name__ == "RustAwaitable"
