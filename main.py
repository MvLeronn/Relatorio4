from database import Database
from save_json import writeAJson

db = Database(database="loja_de_roupas", collection="vendas")
db.resetDatabase()


class ProductAnalyzer:
    def retornar(self):
        result = db.collection.aggregate([
            {"$match": {"cliente_id": "B"}},
            {"$unwind": "$produtos"},
            {"$group": {"_id": "$cliente_id",
                        "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}}
        ])
        writeAJson(result, "total_gasto_b")

    def produtomenosvendio(self):
        result1 = db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": "$produtos.nome", "total": {"$sum": "$produtos.quantidade"}}},
            {"$sort": {"total": 1}},
            {"$limit": 1}
        ])
        writeAJson(result1, "produto_menos_vendido")

    def cllientemenosgasto(self):
        result2 = db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": {"cliente": "$cliente_id",
                                "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}}},
            {"$sort": {"total": 1}},
            {"$limit": 1}
        ])
        writeAJson(result2, "cliente_menos_gastou")

    def listaquantidademaiorque2(self):
        result3 = db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$match": {"produtos.quantidade": {"$gt": 2}}}
        ])
        writeAJson(result3, "quantidade_maior_que_2")


a = ProductAnalyzer()

a.retornar()
a.produtomenosvendio()
a.cllientemenosgasto()
a.listaquantidademaiorque2()
