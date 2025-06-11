from invoke import Collection, Program

from dbt_invoke import mdfiles, properties
from dbt_invoke.internal import _version



ns = Collection()
ns.add_collection(mdfiles)
ns.add_collection(properties)
program = Program(namespace=ns, version=_version.__version__)

