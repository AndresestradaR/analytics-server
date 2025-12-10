"""
Servidor Analytics MCP - v1.0 ANÁLISIS 360°
Orquesta Shopify + Dropi + Meta Ads para análisis completo de rentabilidad COD.

Funciona para Guatemala y Colombia (fletes variables).
"""

import os
import json
import httpx
import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import Response, JSONResponse
from sse_starlette.sse import EventSourceResponse
import uvicorn

load_dotenv()

# ==============================================================================
# CONFIGURACIÓN - URLs de los servidores MCP
# ==============================================================================

SHOPIFY_MCP_URL = os.getenv("SHOPIFY_MCP_URL", "https://mcp-dropshipping-production.up.railway.app")
DROPI_MCP_URL = os.getenv("DROPI_MCP_URL", "https://dropi-production.up.railway.app")
META_MCP_URL = os.getenv("META_MCP_URL", "https://server-meta-production-4773.up.railway.app")
# TIKTOK_MCP_URL = os.getenv("TIKTOK_MCP_URL", "")  # Pendiente aprobación

# Moneda según país (se detecta de Dropi)
CURRENCY_SYMBOL = os.getenv("CURRENCY_SYMBOL", "Q")  # Q para Guatemala, $ para Colombia

sessions = {}

# ==============================================================================
# CLIENTE HTTP PARA LLAMAR A OTROS SERVIDORES MCP
# ==============================================================================

async def call_mcp_tool(server_url: str, tool_name: str, arguments: dict = None) -> dict:
    """Llama a una herramienta en otro servidor MCP."""
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{server_url}/call",
                json={
                    "name": tool_name,
                    "arguments": arguments or {}
                }
            )
            if response.status_code == 200:
                data = response.json()
                result = data.get("result", "")
                
                # Extraer JSON_DATA si existe
                if "---JSON_DATA---" in str(result):
                    parts = str(result).split("---JSON_DATA---")
                    if len(parts) > 1:
                        try:
                            json_data = json.loads(parts[1].strip())
                            return {"success": True, "text": parts[0].strip(), "data": json_data}
                        except:
                            pass
                
                return {"success": True, "text": result, "data": None}
            else:
                return {"success": False, "error": f"HTTP {response.status_code}"}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==============================================================================
# HERRAMIENTAS MCP
# ==============================================================================

TOOLS = [
    {
        "name": "analisis_360",
        "description": """Análisis 360° de rentabilidad para dropshipping COD.
        
Calcula:
- Tasa de cancelación (Shopify vs Dropi)
- Pedidos entregados con ganancias REALES
- Pagos recibidos vs pendientes de pago
- Devoluciones con fletes REALES (variables en Colombia)
- Pedidos pendientes con proyección individual
- Gasto en Meta Ads
- ROAS, CPA real, profit neto

Usa start_date y end_date en formato YYYY-MM-DD.""",
        "inputSchema": {
            "type": "object",
            "properties": {
                "start_date": {
                    "type": "string",
                    "description": "Fecha inicio YYYY-MM-DD (ej: 2025-12-01)"
                },
                "end_date": {
                    "type": "string",
                    "description": "Fecha fin YYYY-MM-DD (ej: 2025-12-15)"
                }
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "proyeccion_pendientes",
        "description": """Calcula escenarios de ganancia/pérdida para pedidos pendientes.
        
Ejemplo: "Si de 30 pendientes se entregan 20 y 10 son devolución, ¿cuánto gano?"
Usa los datos REALES de cada pedido (ganancia y flete individual).""",
        "inputSchema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Fecha inicio YYYY-MM-DD"},
                "end_date": {"type": "string", "description": "Fecha fin YYYY-MM-DD"},
                "escenario_entregas": {"type": "integer", "description": "Cantidad que se entregarían"},
                "escenario_devoluciones": {"type": "integer", "description": "Cantidad que serían devolución"}
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "resumen_rapido",
        "description": "Resumen rápido del día de hoy: ventas, entregas, gasto ads, ganancia.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
]

# ==============================================================================
# IMPLEMENTACIÓN DE HERRAMIENTAS
# ==============================================================================

async def analisis_360(args: dict) -> str:
    """Análisis 360° completo de rentabilidad."""
    
    start_date = args.get("start_date")
    end_date = args.get("end_date")
    
    if not start_date or not end_date:
        return "❌ Se requieren start_date y end_date en formato YYYY-MM-DD"
    
    period_label = f"{start_date} al {end_date}" if start_date != end_date else start_date
    
    result_text = f"📊 ANÁLISIS 360° - {period_label}\n"
    result_text += "=" * 50 + "\n\n"
    
    # -------------------------------------------------------------------------
    # PASO 1: Obtener pedidos de Shopify del período
    # -------------------------------------------------------------------------
    result_text += "⏳ Consultando Shopify...\n"
    
    shopify_result = await call_mcp_tool(
        SHOPIFY_MCP_URL,
        "get_sales_by_period",
        {"start_date": start_date, "end_date": end_date}
    )
    
    shopify_orders_count = 0
    shopify_total_value = 0
    
    if shopify_result.get("success"):
        if shopify_result.get("data"):
            shopify_orders_count = shopify_result["data"].get("order_count", 0)
            shopify_total_value = shopify_result["data"].get("total_sales", 0)
        else:
            # Parsear del texto si no hay JSON
            text = shopify_result.get("text", "")
            if "pedidos" in text.lower():
                import re
                match = re.search(r'(\d+)\s*pedidos?', text.lower())
                if match:
                    shopify_orders_count = int(match.group(1))
    
    # -------------------------------------------------------------------------
    # PASO 2: Obtener pedidos de Dropi del período con datos financieros
    # -------------------------------------------------------------------------
    result_text += "⏳ Consultando Dropi...\n"
    
    dropi_result = await call_mcp_tool(
        DROPI_MCP_URL,
        "get_dropi_orders",
        {"start_date": start_date, "end_date": end_date, "limit": 500}
    )
    
    dropi_orders = []
    dropi_orders_count = 0
    
    if dropi_result.get("success") and dropi_result.get("data"):
        data = dropi_result["data"]
        dropi_orders_count = data.get("total_orders", 0)
        dropi_orders = data.get("orders", [])
    
    # -------------------------------------------------------------------------
    # PASO 3: Obtener detalles financieros de cada pedido de Dropi
    # -------------------------------------------------------------------------
    
    # Clasificar por estado
    entregados = []
    devoluciones = []
    pendientes = []
    cancelados = []
    
    # Si tenemos los IDs, obtener detalles completos
    order_ids = [o.get("id") for o in dropi_orders if o.get("id")]
    
    # Obtener detalles financieros batch (máximo 50)
    if order_ids:
        result_text += f"⏳ Obteniendo detalles financieros de {len(order_ids)} pedidos...\n"
        
        financial_result = await call_mcp_tool(
            DROPI_MCP_URL,
            "get_orders_financial_details",
            {"order_ids": order_ids[:50]}  # Limitar a 50
        )
        
        if financial_result.get("success") and financial_result.get("data"):
            fin_data = financial_result["data"]
            
            for order in fin_data.get("orders", []):
                status = (order.get("status") or "").upper()
                order_info = {
                    "id": order.get("order_id"),
                    "profit": order.get("profit", 0),
                    "shipping_cost": order.get("shipping_cost", 0),
                    "paid": order.get("paid", False),
                    "payment_amount": order.get("payment_amount", 0)
                }
                
                if status in ["ENTREGADO", "DELIVERED", "COMPLETADO"]:
                    entregados.append(order_info)
                elif status in ["DEVOLUCION", "DEVUELTO", "RETURNED", "NO ENTREGADO"]:
                    devoluciones.append(order_info)
                elif status in ["CANCELADO", "CANCELLED"]:
                    cancelados.append(order_info)
                else:
                    pendientes.append(order_info)
    
    # Si no tenemos detalles batch, usar los datos básicos
    if not entregados and not devoluciones and not pendientes:
        for order in dropi_orders:
            status = (order.get("status") or "").upper()
            order_info = {
                "id": order.get("id"),
                "profit": order.get("profit", 0),
                "shipping_cost": 0,  # No tenemos el dato individual
                "paid": False
            }
            
            if status in ["ENTREGADO", "DELIVERED", "COMPLETADO"]:
                entregados.append(order_info)
            elif status in ["DEVOLUCION", "DEVUELTO", "RETURNED", "NO ENTREGADO"]:
                devoluciones.append(order_info)
            elif status in ["CANCELADO", "CANCELLED"]:
                cancelados.append(order_info)
            else:
                pendientes.append(order_info)
    
    # -------------------------------------------------------------------------
    # PASO 4: Obtener historial de wallet para verificar pagos
    # -------------------------------------------------------------------------
    result_text += "⏳ Consultando pagos en wallet...\n"
    
    wallet_result = await call_mcp_tool(
        DROPI_MCP_URL,
        "get_dropi_wallet_history",
        {"start_date": start_date, "end_date": end_date}
    )
    
    total_wallet_income = 0
    total_wallet_expenses = 0
    
    if wallet_result.get("success") and wallet_result.get("data"):
        wallet_data = wallet_result["data"]
        total_wallet_income = wallet_data.get("total_income", 0)
        total_wallet_expenses = wallet_data.get("total_expenses", 0)
    
    # -------------------------------------------------------------------------
    # PASO 5: Obtener gasto de Meta Ads
    # -------------------------------------------------------------------------
    result_text += "⏳ Consultando Meta Ads...\n"
    
    meta_result = await call_mcp_tool(
        META_MCP_URL,
        "get_ad_spend_by_period",
        {"start_date": start_date, "end_date": end_date}
    )
    
    meta_spend = 0
    meta_clicks = 0
    meta_impressions = 0
    
    if meta_result.get("success") and meta_result.get("data"):
        meta_data = meta_result["data"]
        meta_spend = meta_data.get("spend", 0)
        meta_clicks = meta_data.get("clicks", 0)
        meta_impressions = meta_data.get("impressions", 0)
    
    # -------------------------------------------------------------------------
    # PASO 6: CALCULAR MÉTRICAS
    # -------------------------------------------------------------------------
    
    # Cancelaciones
    cancelaciones_pre_envio = max(0, shopify_orders_count - dropi_orders_count)
    cancelaciones_total = cancelaciones_pre_envio + len(cancelados)
    tasa_cancelacion = (cancelaciones_total / shopify_orders_count * 100) if shopify_orders_count > 0 else 0
    
    # Ganancias
    ganancia_entregados = sum(o.get("profit", 0) for o in entregados)
    
    # Pagos
    pagados = [o for o in entregados if o.get("paid")]
    no_pagados = [o for o in entregados if not o.get("paid")]
    monto_pagado = sum(o.get("payment_amount", 0) for o in pagados)
    monto_pendiente_pago = sum(o.get("profit", 0) for o in no_pagados)
    
    # Devoluciones - fletes reales
    fletes_devoluciones = sum(o.get("shipping_cost", 0) for o in devoluciones)
    # Si no tenemos fletes individuales, estimar (Guatemala Q23, Colombia variable)
    if fletes_devoluciones == 0 and len(devoluciones) > 0:
        flete_promedio = 23  # Guatemala default
        fletes_devoluciones = len(devoluciones) * flete_promedio
    
    # Pendientes - proyección
    ganancia_potencial_pendientes = sum(o.get("profit", 0) for o in pendientes)
    fletes_potenciales_pendientes = sum(o.get("shipping_cost", 0) for o in pendientes)
    
    # Profit neto
    ganancia_confirmada = ganancia_entregados - fletes_devoluciones
    profit_neto = ganancia_confirmada - meta_spend
    
    # Métricas
    roas = (ganancia_confirmada / meta_spend) if meta_spend > 0 else 0
    cpa_real = (meta_spend / len(entregados)) if len(entregados) > 0 else 0
    tasa_entrega = (len(entregados) / dropi_orders_count * 100) if dropi_orders_count > 0 else 0
    
    # -------------------------------------------------------------------------
    # CONSTRUIR REPORTE
    # -------------------------------------------------------------------------
    
    result_text = f"""
📊 ANÁLISIS 360° - {period_label}
{'=' * 50}

📦 PEDIDOS
   Shopify: {shopify_orders_count} pedidos
   Dropi: {dropi_orders_count} pedidos
   
   ❌ Cancelaciones pre-envío: {cancelaciones_pre_envio}
   ❌ Cancelados en Dropi: {len(cancelados)}
   📉 Tasa cancelación: {tasa_cancelacion:.1f}%

📊 ESTADOS EN DROPI
   ✅ Entregados: {len(entregados)}
   ❌ Devoluciones: {len(devoluciones)}
   ⏳ Pendientes: {len(pendientes)}
   🚫 Cancelados: {len(cancelados)}
   
   📈 Tasa de entrega: {tasa_entrega:.1f}%

💰 GANANCIAS
   Entregados: {CURRENCY_SYMBOL}{ganancia_entregados:,.2f}
   - Fletes devolución: -{CURRENCY_SYMBOL}{fletes_devoluciones:,.2f}
   = Ganancia confirmada: {CURRENCY_SYMBOL}{ganancia_confirmada:,.2f}

💳 PAGOS DE DROPI
   ✅ Ya pagado: {CURRENCY_SYMBOL}{monto_pagado:,.2f} ({len(pagados)} pedidos)
   ⏳ Por pagar: {CURRENCY_SYMBOL}{monto_pendiente_pago:,.2f} ({len(no_pagados)} pedidos)
"""

    if len(no_pagados) > 0 and len(no_pagados) <= 10:
        result_text += f"   📋 IDs pendientes: {', '.join(['#' + str(o['id']) for o in no_pagados])}\n"

    result_text += f"""
📢 META ADS
   💸 Gasto: {CURRENCY_SYMBOL}{meta_spend:,.2f}
   👆 Clics: {meta_clicks:,}
   👀 Impresiones: {meta_impressions:,}

📈 MÉTRICAS CLAVE
   ROAS: {roas:.2f}x (por cada {CURRENCY_SYMBOL}1 gastado, recuperas {CURRENCY_SYMBOL}{roas:.2f})
   CPA real: {CURRENCY_SYMBOL}{cpa_real:.2f} (costo por entrega efectiva)

💵 RESULTADO FINAL
   Ganancia confirmada: {CURRENCY_SYMBOL}{ganancia_confirmada:,.2f}
   - Gasto Meta Ads: -{CURRENCY_SYMBOL}{meta_spend:,.2f}
   {'=' * 30}
   💰 PROFIT NETO: {CURRENCY_SYMBOL}{profit_neto:,.2f}
"""

    if profit_neto > 0:
        result_text += f"\n   ✅ ¡ESTÁS GANANDO DINERO!\n"
    else:
        result_text += f"\n   ⚠️ ESTÁS EN PÉRDIDA. Revisa tu CPA y tasa de devolución.\n"

    # Sección de pendientes
    if len(pendientes) > 0:
        result_text += f"""
🔮 PROYECCIÓN PENDIENTES ({len(pendientes)} pedidos)
   Si se entregan todos: +{CURRENCY_SYMBOL}{ganancia_potencial_pendientes:,.2f}
   Si todos son devolución: -{CURRENCY_SYMBOL}{fletes_potenciales_pendientes:,.2f}
   
   💡 Usa 'proyeccion_pendientes' para escenarios específicos
"""

    # JSON para integración
    json_data = {
        "period": period_label,
        "shopify_orders": shopify_orders_count,
        "dropi_orders": dropi_orders_count,
        "cancelaciones_pre_envio": cancelaciones_pre_envio,
        "cancelados_dropi": len(cancelados),
        "tasa_cancelacion": round(tasa_cancelacion, 2),
        "entregados": len(entregados),
        "devoluciones": len(devoluciones),
        "pendientes": len(pendientes),
        "tasa_entrega": round(tasa_entrega, 2),
        "ganancia_entregados": round(ganancia_entregados, 2),
        "fletes_devoluciones": round(fletes_devoluciones, 2),
        "ganancia_confirmada": round(ganancia_confirmada, 2),
        "monto_pagado": round(monto_pagado, 2),
        "monto_pendiente_pago": round(monto_pendiente_pago, 2),
        "meta_spend": round(meta_spend, 2),
        "roas": round(roas, 2),
        "cpa_real": round(cpa_real, 2),
        "profit_neto": round(profit_neto, 2),
        "pendientes_ganancia_potencial": round(ganancia_potencial_pendientes, 2),
        "pendientes_fletes_potenciales": round(fletes_potenciales_pendientes, 2),
        "currency": CURRENCY_SYMBOL
    }
    
    result_text += f"\n\n---JSON_DATA---\n{json.dumps(json_data)}"
    
    return result_text


async def proyeccion_pendientes(args: dict) -> str:
    """Calcula escenarios para pedidos pendientes."""
    
    start_date = args.get("start_date")
    end_date = args.get("end_date")
    escenario_entregas = args.get("escenario_entregas")
    escenario_devoluciones = args.get("escenario_devoluciones")
    
    if not start_date or not end_date:
        return "❌ Se requieren start_date y end_date"
    
    # Obtener pedidos pendientes de Dropi
    dropi_result = await call_mcp_tool(
        DROPI_MCP_URL,
        "get_dropi_orders",
        {"start_date": start_date, "end_date": end_date, "limit": 500}
    )
    
    if not dropi_result.get("success"):
        return f"❌ Error consultando Dropi: {dropi_result.get('error')}"
    
    dropi_orders = dropi_result.get("data", {}).get("orders", [])
    
    # Filtrar pendientes
    estados_pendientes = ["PENDIENTE", "GUIA_GENERADA", "RECOLECTADO", "EN_RUTA", "EN BODEGA", "EN TRANSITO"]
    pendientes = [o for o in dropi_orders if (o.get("status") or "").upper() in estados_pendientes or 
                  (o.get("status") or "").upper() not in ["ENTREGADO", "DEVOLUCION", "CANCELADO"]]
    
    if not pendientes:
        return f"✅ No hay pedidos pendientes en el período {start_date} al {end_date}"
    
    # Obtener detalles financieros
    order_ids = [o.get("id") for o in pendientes if o.get("id")]
    
    pendientes_detalle = []
    
    if order_ids:
        financial_result = await call_mcp_tool(
            DROPI_MCP_URL,
            "get_orders_financial_details",
            {"order_ids": order_ids[:50]}
        )
        
        if financial_result.get("success") and financial_result.get("data"):
            pendientes_detalle = financial_result["data"].get("orders", [])
    
    # Si no hay detalles, usar estimaciones
    if not pendientes_detalle:
        ganancia_promedio = 100  # Estimación
        flete_promedio = 23  # Guatemala
        total_pendientes = len(pendientes)
    else:
        total_pendientes = len(pendientes_detalle)
    
    # Calcular totales
    total_ganancia_potencial = sum(o.get("profit", 100) for o in pendientes_detalle) if pendientes_detalle else len(pendientes) * 100
    total_fletes_potenciales = sum(o.get("shipping_cost", 23) for o in pendientes_detalle) if pendientes_detalle else len(pendientes) * 23
    
    ganancia_promedio = total_ganancia_potencial / total_pendientes if total_pendientes > 0 else 0
    flete_promedio = total_fletes_potenciales / total_pendientes if total_pendientes > 0 else 0
    
    result_text = f"""
🔮 PROYECCIÓN DE PENDIENTES
📅 Período: {start_date} al {end_date}
{'=' * 50}

📦 Pedidos pendientes: {total_pendientes}
💰 Ganancia promedio por pedido: {CURRENCY_SYMBOL}{ganancia_promedio:.2f}
🚚 Flete promedio por pedido: {CURRENCY_SYMBOL}{flete_promedio:.2f}

📊 ESCENARIOS:

"""
    
    # Escenario optimista (100% entrega)
    escenario_100 = total_ganancia_potencial
    result_text += f"✅ Si se entregan TODOS ({total_pendientes}):\n"
    result_text += f"   Ganancia: +{CURRENCY_SYMBOL}{escenario_100:,.2f}\n\n"
    
    # Escenario pesimista (100% devolución)
    escenario_0 = -total_fletes_potenciales
    result_text += f"❌ Si TODOS son devolución ({total_pendientes}):\n"
    result_text += f"   Pérdida: -{CURRENCY_SYMBOL}{total_fletes_potenciales:,.2f}\n\n"
    
    # Escenario 80-20
    entregas_80 = int(total_pendientes * 0.8)
    devoluciones_20 = total_pendientes - entregas_80
    ganancia_80 = entregas_80 * ganancia_promedio
    perdida_20 = devoluciones_20 * flete_promedio
    neto_80_20 = ganancia_80 - perdida_20
    
    result_text += f"📈 Escenario 80% entrega / 20% devolución:\n"
    result_text += f"   {entregas_80} entregas × {CURRENCY_SYMBOL}{ganancia_promedio:.2f} = +{CURRENCY_SYMBOL}{ganancia_80:,.2f}\n"
    result_text += f"   {devoluciones_20} devoluciones × {CURRENCY_SYMBOL}{flete_promedio:.2f} = -{CURRENCY_SYMBOL}{perdida_20:,.2f}\n"
    result_text += f"   NETO: {CURRENCY_SYMBOL}{neto_80_20:,.2f}\n\n"
    
    # Escenario 60-40
    entregas_60 = int(total_pendientes * 0.6)
    devoluciones_40 = total_pendientes - entregas_60
    ganancia_60 = entregas_60 * ganancia_promedio
    perdida_40 = devoluciones_40 * flete_promedio
    neto_60_40 = ganancia_60 - perdida_40
    
    result_text += f"📊 Escenario 60% entrega / 40% devolución:\n"
    result_text += f"   {entregas_60} entregas × {CURRENCY_SYMBOL}{ganancia_promedio:.2f} = +{CURRENCY_SYMBOL}{ganancia_60:,.2f}\n"
    result_text += f"   {devoluciones_40} devoluciones × {CURRENCY_SYMBOL}{flete_promedio:.2f} = -{CURRENCY_SYMBOL}{perdida_40:,.2f}\n"
    result_text += f"   NETO: {CURRENCY_SYMBOL}{neto_60_40:,.2f}\n\n"
    
    # Escenario personalizado
    if escenario_entregas is not None and escenario_devoluciones is not None:
        ganancia_custom = escenario_entregas * ganancia_promedio
        perdida_custom = escenario_devoluciones * flete_promedio
        neto_custom = ganancia_custom - perdida_custom
        
        result_text += f"🎯 TU ESCENARIO ({escenario_entregas} entregas / {escenario_devoluciones} devoluciones):\n"
        result_text += f"   Ganancia: +{CURRENCY_SYMBOL}{ganancia_custom:,.2f}\n"
        result_text += f"   Pérdida fletes: -{CURRENCY_SYMBOL}{perdida_custom:,.2f}\n"
        result_text += f"   NETO: {CURRENCY_SYMBOL}{neto_custom:,.2f}\n\n"
    
    # Punto de equilibrio
    if ganancia_promedio > 0 and flete_promedio > 0:
        # Punto donde ganancia = pérdida
        # x * ganancia_promedio = (total - x) * flete_promedio
        # x * ganancia + x * flete = total * flete
        # x = (total * flete) / (ganancia + flete)
        punto_equilibrio = (total_pendientes * flete_promedio) / (ganancia_promedio + flete_promedio)
        porcentaje_minimo = (punto_equilibrio / total_pendientes) * 100
        
        result_text += f"⚖️ PUNTO DE EQUILIBRIO:\n"
        result_text += f"   Necesitas entregar al menos {punto_equilibrio:.0f} pedidos ({porcentaje_minimo:.1f}%)\n"
        result_text += f"   para no perder dinero en este lote.\n"
    
    return result_text


async def resumen_rapido(args: dict) -> str:
    """Resumen rápido del día de hoy."""
    
    from datetime import date
    today = date.today().isoformat()
    
    result_text = f"📊 RESUMEN RÁPIDO - {today}\n"
    result_text += "=" * 40 + "\n\n"
    
    # Shopify hoy
    shopify_result = await call_mcp_tool(
        SHOPIFY_MCP_URL,
        "get_total_sales_today",
        {}
    )
    
    if shopify_result.get("success"):
        result_text += f"🛒 SHOPIFY:\n{shopify_result.get('text', 'Sin datos')}\n\n"
    
    # Dropi hoy
    dropi_result = await call_mcp_tool(
        DROPI_MCP_URL,
        "get_dropi_orders",
        {"days": 1}
    )
    
    if dropi_result.get("success"):
        data = dropi_result.get("data", {})
        result_text += f"📦 DROPI:\n"
        result_text += f"   Pedidos: {data.get('total_orders', 0)}\n"
        result_text += f"   Entregados: {data.get('delivered', 0)}\n"
        result_text += f"   Devoluciones: {data.get('returned', 0)}\n"
        result_text += f"   Pendientes: {data.get('pending', 0)}\n\n"
    
    # Meta hoy
    meta_result = await call_mcp_tool(
        META_MCP_URL,
        "get_ad_spend_today",
        {}
    )
    
    if meta_result.get("success"):
        result_text += f"📢 META ADS:\n{meta_result.get('text', 'Sin datos')}\n\n"
    
    # Wallet
    wallet_result = await call_mcp_tool(
        DROPI_MCP_URL,
        "get_dropi_wallet",
        {}
    )
    
    if wallet_result.get("success"):
        result_text += f"💰 WALLET:\n{wallet_result.get('text', 'Sin datos')}\n"
    
    return result_text


# ==============================================================================
# DISPATCHER
# ==============================================================================

TOOL_HANDLERS = {
    "analisis_360": analisis_360,
    "proyeccion_pendientes": proyeccion_pendientes,
    "resumen_rapido": resumen_rapido,
}

async def execute_tool(name: str, args: dict) -> str:
    handler = TOOL_HANDLERS.get(name)
    if handler:
        try:
            return await handler(args)
        except Exception as e:
            import traceback
            return f"Error ejecutando {name}: {str(e)}\n{traceback.format_exc()}"
    return f"Herramienta '{name}' no encontrada"

# ==============================================================================
# ENDPOINTS HTTP
# ==============================================================================

async def http_tools(request):
    return JSONResponse({"tools": TOOLS})

async def http_call_tool(request):
    body = await request.json()
    name = body.get("name", "")
    args = body.get("arguments", {})
    result = await execute_tool(name, args)
    return JSONResponse({"result": result})

async def sse_endpoint(request):
    queue = asyncio.Queue()
    session_id = str(id(queue))
    sessions[session_id] = queue
    
    async def gen():
        try:
            yield {"event": "endpoint", "data": f"/messages/{session_id}"}
            while True:
                data = await queue.get()
                yield {"event": "message", "data": json.dumps(data)}
        except asyncio.CancelledError:
            pass
        finally:
            sessions.pop(session_id, None)
    
    return EventSourceResponse(gen())

async def messages_endpoint(request):
    session_id = request.path_params["session_id"]
    if session_id not in sessions:
        return Response("Not found", status_code=404)
    
    body = await request.json()
    method = body.get("method", "")
    msg_id = body.get("id")
    
    if method == "initialize":
        resp = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "analytics-mcp", "version": "1.0.0"}
            }
        }
    elif method == "tools/list":
        resp = {"jsonrpc": "2.0", "id": msg_id, "result": {"tools": TOOLS}}
    elif method == "tools/call":
        params = body.get("params", {})
        result = await execute_tool(params.get("name", ""), params.get("arguments", {}))
        resp = {"jsonrpc": "2.0", "id": msg_id, "result": {"content": [{"type": "text", "text": result}]}}
    else:
        resp = {"jsonrpc": "2.0", "id": msg_id, "result": {}}
    
    if resp and msg_id:
        await sessions[session_id].put(resp)
    
    return Response("OK")

async def health(request):
    return JSONResponse({
        "status": "ok",
        "version": "1.0.0",
        "service": "Analytics MCP - Análisis 360°",
        "tools": len(TOOLS)
    })

# ==============================================================================
# APP
# ==============================================================================

app = Starlette(routes=[
    Route("/", health),
    Route("/health", health),
    Route("/tools", http_tools),
    Route("/call", http_call_tool, methods=["POST"]),
    Route("/sse", sse_endpoint),
    Route("/messages/{session_id}", messages_endpoint, methods=["POST"]),
])

if __name__ == "__main__":
    port = int(os.getenv("PORT", 3000))
    print(f"🚀 Analytics MCP Server v1.0 - Análisis 360°")
    print(f"📊 Herramientas: {', '.join(TOOL_HANDLERS.keys())}")
    print(f"🔗 Shopify: {SHOPIFY_MCP_URL}")
    print(f"🔗 Dropi: {DROPI_MCP_URL}")
    print(f"🔗 Meta: {META_MCP_URL}")
    uvicorn.run(app, host="0.0.0.0", port=port)
