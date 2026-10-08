import sqlite3
import unittest

from views.resumen_pedidos import RESUMEN_PEDIDOS_SQL, preparar_pedidos


class ResumenPedidosTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        self.conn.execute("""
            CREATE TABLE pedxclixprod (
                bd TEXT, codigo_cli TEXT, nombre TEXT, barrio TEXT,
                ciudad TEXT, asesor TEXT, codigo_pideky TEXT,
                numero_pedido TEXT, valor NUMERIC, ruta INTEGER
            )
        """)

    def resumen(self, empresa="empresa"):
        # El SELECT utiliza agregaciones estándar compartidas con PostgreSQL.
        return self.conn.execute(
            RESUMEN_PEDIDOS_SQL.replace("%s", "?"), (empresa,)
        ).fetchall()

    def test_consolida_cliente_con_descripciones_distintas(self):
        self.conn.executemany(
            "INSERT INTO pedxclixprod VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("empresa", "001", "Tienda", "San Jose", "Cali", "01", "00012", "P1", 100, 7),
                ("empresa", "001", "TIENDA", "San José", "CALI", "02", "00012", "P1", 200, 7),
                ("empresa", "001", None, None, None, None, "00013", "P2", 50, 7),
                ("empresa", "002", "Tienda", "San Jose", "Cali", "01", "00014", "P3", 20, None),
                ("otra", "001", "Tienda", "San Jose", "Cali", "01", "00015", "P4", 999, 8),
            ],
        )
        rows = self.resumen()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0:2], ("empresa", "001"))
        self.assertTrue(all(rows[0][i] for i in range(2, 6)))
        self.assertEqual(rows[0][6:], ("00013", 2, 350, 7))
        self.assertEqual(rows[1][1], "002")
        self.assertEqual(rows[1][7:], (1, 20, None))

    def test_empresa_sin_pedidos(self):
        self.assertEqual(self.resumen(), [])

    def test_limpieza_preserva_identificador_y_datos_del_pedido(self):
        cliente = "000123-" + "CLIENTE " * 10
        pedido = {
            "cliente": cliente, "nombre": "N" * 50, "barrio": "B" * 50,
            "numero_pedido": "000001", "codigo_pideky": "000002", "valor": 123,
        }
        preparar_pedidos([pedido])
        self.assertEqual(pedido["cliente"], cliente)
        self.assertEqual(pedido["nombre"], "N" * 40)
        self.assertEqual(pedido["barrio"], "B" * 40)
        self.assertEqual(pedido["numero_pedido"], "000001")
        self.assertEqual(pedido["codigo_pideky"], "000002")
        self.assertEqual(pedido["valor"], 123)

    def test_campos_descriptivos_ausentes_o_nulos(self):
        pedidos = [{"cliente": 123}, {"nombre": None, "barrio": None}]
        preparar_pedidos(pedidos)
        self.assertEqual(pedidos, [{"cliente": 123}, {"nombre": None, "barrio": None}])


if __name__ == "__main__":
    unittest.main()
