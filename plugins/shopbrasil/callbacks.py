"""Callbacks de ciclo de vida e alertas.

O envio de alerta é SIMULADO (log estruturado). Em produção, trocar `_enviar_alerta`
por Slack/Teams/e-mail mantendo a mesma assinatura.
"""

import json
import logging

log = logging.getLogger("shopbrasil.alertas")


def _resumo(context) -> dict:
    ti = context["task_instance"]
    return {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "map_index": ti.map_index,
        "run_id": context.get("run_id"),
        "tentativa": ti.try_number,
        "max_tentativas": ti.max_tries + 1,
        "log_url": ti.log_url,
    }


def _enviar_alerta(canal: str, titulo: str, payload: dict) -> None:
    log.error("[ALERTA SIMULADO] canal=%s | %s | %s", canal, titulo, json.dumps(payload, default=str))


def ao_sucesso(context) -> None:
    info = _resumo(context)
    log.info("Task %s concluída com sucesso na tentativa %s.", info["task_id"], info["tentativa"])


def ao_retentar(context) -> None:
    info = _resumo(context)
    log.warning(
        "Task %s falhou na tentativa %s/%s, vai retentar com backoff. Erro: %s",
        info["task_id"],
        info["tentativa"],
        info["max_tentativas"],
        context.get("exception"),
    )


def ao_falhar(context) -> None:
    info = _resumo(context)
    info["erro"] = repr(context.get("exception"))
    _enviar_alerta("#pricing-alertas", f"Falha definitiva em {info['task_id']}", info)


def sla_perdido(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    _enviar_alerta(
        "#pricing-alertas",
        f"SLA perdido na DAG {dag.dag_id}",
        {"tasks": task_list, "bloqueando": blocking_task_list},
    )
