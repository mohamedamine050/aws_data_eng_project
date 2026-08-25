"""``if __name__ == "__main__":`` doit etre la DERNIERE instruction du module.

Panne reelle, et une regression que j'ai introduite : une fonction ajoutee
*apres* le garde d'execution. Le module s'execute de haut en bas, donc Glue
lance ``main()`` au moment du garde — pendant que la fonction plus bas n'existe
pas encore :

    NameError: name 'log_io' is not defined

Rien ne l'attrape a l'import : le fichier est syntaxiquement valide, pyflakes
ne voit pas d'erreur, et tous les tests unitaires passent parce qu'ils importent
le module au lieu de l'executer. Seul un vrai run Glue le revele.
"""

import ast
import pathlib

import pytest

SRC = pathlib.Path(__file__).resolve().parent.parent / "src"

MODULES = sorted(
    [*(SRC / "jobs").glob("glue_*.py"), *SRC.glob("lambdas/*/handler.py")],
    key=lambda path: path.name,
)


def _main_guard(tree):
    """L'instruction ``if __name__ == "__main__":``, ou None."""
    for node in tree.body:
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if (isinstance(test, ast.Compare)
                and isinstance(test.left, ast.Name) and test.left.id == "__name__"
                and isinstance(test.comparators[0], ast.Constant)
                and test.comparators[0].value == "__main__"):
            return node
    return None


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_nothing_is_defined_after_the_main_guard(path):
    """Tout ce que ``main()`` appelle doit exister quand le garde est atteint."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    guard = _main_guard(tree)

    if guard is None:
        pytest.skip(f"{path.name} n'a pas de garde d'execution")

    after = [
        node for node in tree.body
        if getattr(node, "lineno", 0) > guard.lineno
    ]
    names = [
        getattr(node, "name", type(node).__name__)
        for node in after
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Assign))
    ]

    assert not names, (
        f"{path.name} definit {names} APRES `if __name__ == \"__main__\"` — "
        f"un run Glue leverait NameError avant d'avoir commence"
    )


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_every_name_main_uses_is_defined_before_the_guard(path):
    """Le meme controle, par les noms plutot que par la position.

    Attrape aussi le cas ou ``main()`` appelle une fonction qui n'existe nulle
    part — la faute d'origine de cette serie, ``LOGGER`` puis ``glue_entrypoint``.
    """
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    guard = _main_guard(tree)
    if guard is None:
        pytest.skip(f"{path.name} n'a pas de garde d'execution")

    defined_before = set()
    for node in tree.body:
        if getattr(node, "lineno", 0) >= guard.lineno:
            break
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined_before.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    defined_before.add(target.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                defined_before.add((alias.asname or alias.name).split(".")[0])
        elif isinstance(node, ast.Try):
            for inner in ast.walk(node):
                if isinstance(inner, (ast.Import, ast.ImportFrom)):
                    for alias in inner.names:
                        defined_before.add((alias.asname or alias.name).split(".")[0])
                elif isinstance(inner, ast.Assign):
                    for target in inner.targets:
                        if isinstance(target, ast.Name):
                            defined_before.add(target.id)

    called = {
        node.func.id
        for node in ast.walk(guard)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    builtins = {"print", "len", "str", "int", "dict", "list"}

    missing = sorted(called - defined_before - builtins)

    assert not missing, f"{path.name}: le garde appelle {missing}, non defini au-dessus"


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_the_contract_helpers_are_reachable_from_main(path):
    """``log_io`` tourne au tout debut du travail : il doit exister avant.

    C'est le nom precis qui a casse le run du 25/08, apres que je l'ai ajoute
    en fin de fichier.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    guard = _main_guard(tree)

    definitions = {
        node.name: node.lineno
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    }

    for helper in ("describe_io", "log_io"):
        assert helper in definitions, f"{path.name} ne declare pas {helper}"
        if guard is not None:
            assert definitions[helper] < guard.lineno, (
                f"{path.name}: {helper} est defini apres le garde d'execution"
            )
