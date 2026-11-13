"""Operador customizado que valida o schema dos produtos antes do processamento."""

from __future__ import annotations

from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import BaseOperator


class ValidarProdutosOperator(BaseOperator):
    """Valida cada produto e devolve só os válidos, com os campos usados no pipeline.

    - Produto inválido é descartado e logado.
    - Se a taxa de inválidos passar da tolerância (ou nada sobrar), falha SEM retry:
      schema quebrado não se resolve retentando, precisa de gente olhando.

    :param produtos: lista de produtos (aceita XComArg; é campo templated).
    :param tolerancia_invalidos: fração máxima de produtos inválidos aceita (0 a 1).
    """

    template_fields = ("produtos",)
    ui_color = "#fde2b6"

    # campo -> tipos aceitos
    SCHEMA: dict[str, tuple[type, ...]] = {
        "id": (int,),
        "title": (str,),
        "price": (int, float),
        "category": (str,),
    }

    def __init__(self, *, produtos: Any, tolerancia_invalidos: float = 0.2, **kwargs) -> None:
        super().__init__(**kwargs)
        self.produtos = produtos
        self.tolerancia_invalidos = tolerancia_invalidos

    def _erros(self, produto: Any) -> list[str]:
        if not isinstance(produto, dict):
            return ["registro não é um objeto"]
        erros = []
        for campo, tipos in self.SCHEMA.items():
            valor = produto.get(campo)
            # bool é subclasse de int em Python, então é barrado explicitamente
            if valor is None or isinstance(valor, bool) or not isinstance(valor, tipos):
                erros.append(f"{campo} ausente ou com tipo inválido ({valor!r})")
        if not erros:
            if produto["price"] <= 0:
                erros.append(f"price deve ser > 0 ({produto['price']!r})")
            if not produto["category"].strip():
                erros.append("category vazia")
        return erros

    def execute(self, context) -> list[dict]:
        if not isinstance(self.produtos, list) or not self.produtos:
            raise AirflowFailException("Nenhum produto recebido para validação.")

        validos, invalidos = [], 0
        for produto in self.produtos:
            erros = self._erros(produto)
            if erros:
                invalidos += 1
                pid = produto.get("id") if isinstance(produto, dict) else None
                self.log.warning("Produto %s descartado: %s", pid, "; ".join(erros))
                continue
            validos.append(
                {
                    "id": produto["id"],
                    "title": produto["title"],
                    "price": float(produto["price"]),
                    "category": produto["category"].strip(),
                }
            )

        total = len(self.produtos)
        taxa = invalidos / total
        self.log.info("Validação: %s válidos, %s inválidos de %s (%.0f%%).", len(validos), invalidos, total, taxa * 100)

        if not validos or taxa > self.tolerancia_invalidos:
            raise AirflowFailException(
                f"Schema fora do esperado: {invalidos}/{total} inválidos "
                f"(tolerância {self.tolerancia_invalidos:.0%})."
            )
        return validos
