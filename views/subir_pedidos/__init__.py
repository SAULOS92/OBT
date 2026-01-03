"""Blueprint de subir pedidos y registro de rutas."""

from flask import Blueprint

subir_pedidos_bp = Blueprint(
    "subir_pedidos", __name__, template_folder="../../templates"
)

# Importa las rutas para adjuntarlas al blueprint.
from . import routes  # noqa: E402,F401
