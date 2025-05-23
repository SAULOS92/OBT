import os
from flask import Flask
from views.upload import upload_bp
from views.generar_pedidos import generar_pedidos_bp
from views.consolidar_compras import consolidar_bp
from views.auth import auth_bp
from views.auditoria import auditoria_bp



app = Flask(__name__, static_folder="static")
app.secret_key = os.getenv("SECRET_KEY", "cámbiala")
app.register_blueprint(auth_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(generar_pedidos_bp)
app.register_blueprint(consolidar_bp)
app.register_blueprint(auditoria_bp)



if __name__ == "__main__":
    app.run(
      host="0.0.0.0",
      port=int(os.getenv("PORT", 5000)),
      debug=True
    )
