"""
Analytics Server - Servidor de Análisis de Rentabilidad COD
Cruza datos de Shopify, Dropi, Meta y TikTok para calcular rentabilidad real.

Arquitectura:
- Este servidor NO extrae datos directamente de APIs
- Llama a los otros MCP servers vía HTTP para obtener datos
- Su trabajo es CRUZAR y CALCULAR

Herramientas:
- get_profitability_report: Rentabilidad por fecha de venta o cobro
- get_funnel_analysis: Análisis del embudo Shopify → Dropi → Entregado
- get_leakage_report: Dónde se pierde dinero
- compare_periods: Comparar dos períodos
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

# URLs de los otros MCP servers
DROPI_URL = os.getenv("DROPI_MCP_URL", "https://dropi-production.up.railway.app")
SHOPIFY_URL = os.getenv("SHOPIFY_MCP_URL", "https://mcp-dropshipping-production.up.railway.app")
META_URL = os.getenv("META_MCP_URL", "https://server-meta-production-4773.up.railway.app")
TIKTOK_URL = os.getenv("TIKTOK_MCP_URL", "")  # Pendiente

# Costos fijos del negocio (configurables)
COSTO_PRODUCTO = float(os.getenv("COSTO_PRODUCTO", "50"))  # Q50 por producto
COSTO_ENVIO = float(os.getenv("COSTO_ENVIO", "30"))  # Q30 envío
COSTO_DEVOLUCION = float(os.getenv("COSTO_DEVOLUCION", "23"))  # Q23 por devolución

sessions = {}

# ========== DEFINICIÓN DE HERRAMIENTAS ==========

TOOLS = [
    {
        "name": "get_profitability_report",
        "description": "Genera reporte de rentabilidad cruzando Shopify, Dropi y Meta/TikTok. Puede analizar por fecha de VENTA (cuando se creó el pedido) o por fecha de COBRO (cuando se recibió el pago).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Fecha inicio YYYY-MM-DD"},
                "end_date": {"type": "string", "description": "Fecha fin YYYY-MM-DD"},
                "perspective": {
                    "type": "string", 
                    "description": "Perspectiva de análisis: 'sale' (fecha de venta) o 'payment' (fecha de cobro). Default: sale",
                    "enum": ["sale", "payment"]
                }
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "get_funnel_analysis",
        "description": "Analiza el embudo de conversión: Shopify → Dropi → Entregado. Muestra tasas de conversión y dónde se pierden pedidos.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Fecha inicio YYYY-MM-DD"},
                "end_date": {"type": "string", "description": "Fecha fin YYYY-MM-DD"}
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "get_leakage_report",
        "description": "Identifica fugas de dinero: cancelaciones, devoluciones, y su impacto en el CPA real.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Fecha inicio YYYY-MM-DD"},
                "end_date": {"type": "string", "description": "Fecha fin YYYY-MM-DD"}
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "compare_periods",
        "description": "Compara rentabilidad entre dos períodos diferentes.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "period1_start": {"type": "string", "description": "Inicio período 1 YYYY-MM-DD"},
                "period1_end": {"type": "string", "description": "Fin período 1 YYYY-MM-DD"},
                "period2_start": {"type": "string", "description": "Inicio período 2 YYYY-MM-DD"},
                "period2_end": {"type": "string", "description": "Fin período 2 YYYY-MM-DD"}
            },
            "required": ["period1_start", "period1_end", "period2_start", "period2_end"]
        }
    }
]

# ========== FUNCIONES AUXILIARES PARA LLAMAR A OTROS SERVERS ==========

async def call_mcp_server(server_url: str, tool_name: str, arguments: dict) -> dict:
    """Llama a otro MCP server y retorna el resultado."""
    url = f"{server_url}/call"
    payload = {
        "name": tool_name,
        "arguments": arguments
    }
    
    print(f"📡 Llamando a {server_url} - {tool_name} con {arguments}")
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            response = await client.post(url, json=payload)
            data = response.json()
            print(f"📡 Respuesta: {str(data)[:200]}...")
            return data
        except Exception as e:
            print(f"❌ Error llamando a {server_url}: {str(e)}")
            return {"error": str(e)}

async def get_shopify_orders(start_date: str, end_date: str) -> dict:
    """Obtiene pedidos de Shopify en un rango de fechas."""
    result = await call_mcp_server(
        SHOPIFY_URL,
        "get_sales_by_period",
        {"start_date": start_date, "end_date": end_date}
    )
    return result

async def get_dropi_orders(start_date: str, end_date: str) -> dict:
    """Obtiene órdenes de Dropi en un rango de fechas."""
    result = await call_mcp_server(
        DROPI_URL,
        "get_dropi_orders",
        {"start_date": start_date, "end_date": end_date}
    )
    return result

async def get_dropi_wallet_history(start_date: str, end_date: str) -> dict:
    """Obtiene historial de cartera de Dropi (pagos recibidos)."""
    result = await call_mcp_server(
        DROPI_URL,
        "get_dropi_wallet_history",
        {"start_date": start_date, "end_date": end_date}
    )
    return result

async def get_meta_spend(start_date: str, end_date: str) -> dict:
    """Obtiene gasto de Meta Ads en un rango de fechas."""
    result = await call_mcp_server(
        META_URL,
        "get_ad_spend_by_period",
        {"start_date": start_date, "end_date": end_date}
    )
    return result

async def get_tiktok_spend(start_date: str, end_date: str) -> dict:
    """Obtiene gasto de TikTok Ads en un rango de fechas."""
    if not TIKTOK_URL:
        return {"error": "TikTok no configurado"}
    result = await call_mcp_server(
        TIKTOK_URL,
        "get_ad_spend_by_period",
        {"start_date": start_date, "end_date": end_date}
    )
    return result

# ========== FUNCIONES DE PARSING ==========

def parse_json_from_response(response: dict) -> dict:
    """Extrae datos JSON de la respuesta de un MCP server."""
    result = response.get("result", "")
    
    # Buscar JSON_DATA en la respuesta
    if "---JSON_DATA---" in result:
        json_str = result.split("---JSON_DATA---")[1].strip()
        try:
            return json.loads(json_str)
        except:
            pass
    
    # Si no hay JSON_DATA, intentar parsear números del texto
    return {"raw": result}

def extract_spend_from_meta_response(response: dict) -> float:
    """Extrae el gasto de la respuesta de Meta."""
    result = response.get("result", "")
    
    # Buscar JSON_DATA
    if "---JSON_DATA---" in result:
        try:
            json_str = result.split("---JSON_DATA---")[1].strip()
            data = json.loads(json_str)
            return float(data.get("spend", 0))
        except:
            pass
    
    # Buscar patrón "Gasto: $X,XXX.XX" en el texto
    import re
    match = re.search(r'Gasto:\s*\$?([\d,]+\.?\d*)', result)
    if match:
        return float(match.group(1).replace(",", ""))
    
    return 0.0

def extract_dropi_stats(response: dict) -> dict:
    """Extrae estadísticas de la respuesta de Dropi orders."""
    result = response.get("result", "")
    
    # Buscar JSON_DATA
    if "---JSON_DATA---" in result:
        try:
            json_str = result.split("---JSON_DATA---")[1].strip()
            return json.loads(json_str)
        except:
            pass
    
    # Parsing manual del texto
    import re
    stats = {
        "total_orders": 0,
        "delivered": 0,
        "returned": 0,
        "pending": 0,
        "in_transit": 0
    }
    
    # Buscar patrones comunes
    total_match = re.search(r'Total[:\s]+(\d+)', result)
    if total_match:
        stats["total_orders"] = int(total_match.group(1))
    
    delivered_match = re.search(r'[Ee]ntregad[oa]s?[:\s]+(\d+)', result)
    if delivered_match:
        stats["delivered"] = int(delivered_match.group(1))
    
    returned_match = re.search(r'[Dd]evuel[tc][oa]s?[:\s]+(\d+)', result)
    if returned_match:
        stats["returned"] = int(returned_match.group(1))
    
    return stats

def extract_wallet_stats(response: dict) -> dict:
    """Extrae estadísticas del historial de cartera."""
    result = response.get("result", "")
    
    # Buscar JSON_DATA
    if "---JSON_DATA---" in result:
        try:
            json_str = result.split("---JSON_DATA---")[1].strip()
            return json.loads(json_str)
        except:
            pass
    
    import re
    stats = {
        "total_income": 0,
        "total_expenses": 0,
        "net": 0,
        "entries": 0,
        "exits": 0
    }
    
    # Buscar patrón de entradas
    income_match = re.search(r'[Ee]ntradas?[:\s]*Q?([\d,]+\.?\d*)', result)
    if income_match:
        stats["total_income"] = float(income_match.group(1).replace(",", ""))
    
    # Buscar patrón de salidas
    expense_match = re.search(r'[Ss]alidas?[:\s]*Q?([\d,]+\.?\d*)', result)
    if expense_match:
        stats["total_expenses"] = float(expense_match.group(1).replace(",", ""))
    
    stats["net"] = stats["total_income"] - stats["total_expenses"]
    
    return stats

def extract_shopify_stats(response: dict) -> dict:
    """Extrae estadísticas de Shopify."""
    result = response.get("result", "")
    
    # Buscar JSON_DATA
    if "---JSON_DATA---" in result:
        try:
            json_str = result.split("---JSON_DATA---")[1].strip()
            return json.loads(json_str)
        except:
            pass
    
    import re
    stats = {
        "total_orders": 0,
        "total_sales": 0,
        "paid_orders": 0
    }
    
    orders_match = re.search(r'[Pp]edidos[:\s]+(\d+)', result)
    if orders_match:
        stats["total_orders"] = int(orders_match.group(1))
    
    sales_match = re.search(r'[Tt]otal[:\s]*\$?([\d,]+\.?\d*)', result)
    if sales_match:
        stats["total_sales"] = float(sales_match.group(1).replace(",", ""))
    
    return stats

# ========== HERRAMIENTAS PRINCIPALES ==========

async def get_profitability_report(args: dict) -> str:
    """Genera reporte de rentabilidad completo."""
    print(f"📊 ANALYTICS get_profitability_report - Args: {args}")
    
    if "arguments" in args:
        args = args["arguments"]
    
    start_date = args.get("start_date")
    end_date = args.get("end_date")
    perspective = args.get("perspective", "sale")
    
    if not start_date or not end_date:
        return "❌ Error: Se requieren start_date y end_date"
    
    # Obtener datos de todos los servicios en paralelo
    shopify_task = get_shopify_orders(start_date, end_date)
    dropi_task = get_dropi_orders(start_date, end_date)
    wallet_task = get_dropi_wallet_history(start_date, end_date)
    meta_task = get_meta_spend(start_date, end_date)
    
    shopify_response, dropi_response, wallet_response, meta_response = await asyncio.gather(
        shopify_task, dropi_task, wallet_task, meta_task
    )
    
    # Parsear respuestas
    shopify_stats = extract_shopify_stats(shopify_response)
    dropi_stats = extract_dropi_stats(dropi_response)
    wallet_stats = extract_wallet_stats(wallet_response)
    meta_spend = extract_spend_from_meta_response(meta_response)
    
    # Intentar TikTok si está configurado
    tiktok_spend = 0.0
    if TIKTOK_URL:
        tiktok_response = await get_tiktok_spend(start_date, end_date)
        tiktok_spend = extract_spend_from_meta_response(tiktok_response)
    
    total_ad_spend = meta_spend + tiktok_spend
    
    # Calcular métricas
    total_orders = shopify_stats.get("total_orders", 0)
    delivered = dropi_stats.get("delivered", 0)
    returned = dropi_stats.get("returned", 0)
    
    # Ingresos reales (de wallet o estimado)
    if perspective == "payment":
        revenue = wallet_stats.get("total_income", 0)
        return_costs = wallet_stats.get("total_expenses", 0)
    else:
        # Estimación basada en entregas
        revenue = delivered * 200  # Precio promedio estimado
        return_costs = returned * COSTO_DEVOLUCION
    
    # Costos
    product_costs = delivered * COSTO_PRODUCTO
    shipping_costs = (delivered + returned) * COSTO_ENVIO
    
    # Cálculos finales
    total_costs = product_costs + shipping_costs + return_costs + total_ad_spend
    gross_profit = revenue - total_costs
    
    # Métricas clave
    cpa_initial = total_ad_spend / total_orders if total_orders > 0 else 0
    cpa_real = total_ad_spend / delivered if delivered > 0 else 0
    delivery_rate = (delivered / total_orders * 100) if total_orders > 0 else 0
    roi = (gross_profit / total_ad_spend * 100) if total_ad_spend > 0 else 0
    
    # Construir reporte
    perspective_label = "FECHA DE VENTA" if perspective == "sale" else "FECHA DE COBRO"
    
    report = f"""📊 *REPORTE DE RENTABILIDAD*
📅 Período: {start_date} a {end_date}
🔍 Perspectiva: {perspective_label}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📦 *PEDIDOS*
• Shopify: {total_orders} pedidos
• Entregados: {delivered}
• Devueltos: {returned}
• Tasa de entrega: {delivery_rate:.1f}%

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💰 *INGRESOS*
• Revenue: Q{revenue:,.2f}

💸 *COSTOS*
• Productos: Q{product_costs:,.2f}
• Envíos: Q{shipping_costs:,.2f}
• Devoluciones: Q{return_costs:,.2f}
• Meta Ads: Q{meta_spend:,.2f}"""
    
    if tiktok_spend > 0:
        report += f"\n• TikTok Ads: Q{tiktok_spend:,.2f}"
    
    report += f"""
• *Total costos: Q{total_costs:,.2f}*

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 *RENTABILIDAD*
• Ganancia bruta: Q{gross_profit:,.2f}
• ROI: {roi:.1f}%

🎯 *CPA*
• CPA Inicial: Q{cpa_initial:.2f} (por pedido)
• CPA Real: Q{cpa_real:.2f} (por entrega)
• Diferencia: +Q{cpa_real - cpa_initial:.2f} ({((cpa_real/cpa_initial - 1) * 100) if cpa_initial > 0 else 0:.0f}% más)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""
    
    # JSON para dashboard
    report_json = {
        "period": {"start": start_date, "end": end_date},
        "perspective": perspective,
        "orders": {
            "shopify": total_orders,
            "delivered": delivered,
            "returned": returned,
            "delivery_rate": delivery_rate
        },
        "revenue": revenue,
        "costs": {
            "products": product_costs,
            "shipping": shipping_costs,
            "returns": return_costs,
            "meta_ads": meta_spend,
            "tiktok_ads": tiktok_spend,
            "total": total_costs
        },
        "profit": gross_profit,
        "roi": roi,
        "cpa": {
            "initial": cpa_initial,
            "real": cpa_real
        }
    }
    
    report += f"\n\n---JSON_DATA---\n{json.dumps(report_json)}"
    
    return report

async def get_funnel_analysis(args: dict) -> str:
    """Analiza el embudo de conversión."""
    print(f"📊 ANALYTICS get_funnel_analysis - Args: {args}")
    
    if "arguments" in args:
        args = args["arguments"]
    
    start_date = args.get("start_date")
    end_date = args.get("end_date")
    
    if not start_date or not end_date:
        return "❌ Error: Se requieren start_date y end_date"
    
    # Obtener datos
    shopify_response = await get_shopify_orders(start_date, end_date)
    dropi_response = await get_dropi_orders(start_date, end_date)
    
    shopify_stats = extract_shopify_stats(shopify_response)
    dropi_stats = extract_dropi_stats(dropi_response)
    
    shopify_orders = shopify_stats.get("total_orders", 0)
    dropi_total = dropi_stats.get("total_orders", 0)
    delivered = dropi_stats.get("delivered", 0)
    returned = dropi_stats.get("returned", 0)
    in_transit = dropi_stats.get("in_transit", 0)
    pending = dropi_stats.get("pending", 0)
    
    # Calcular tasas
    shopify_to_dropi = (dropi_total / shopify_orders * 100) if shopify_orders > 0 else 0
    dropi_to_delivered = (delivered / dropi_total * 100) if dropi_total > 0 else 0
    return_rate = (returned / dropi_total * 100) if dropi_total > 0 else 0
    cancelled_in_shopify = shopify_orders - dropi_total
    
    report = f"""🔄 *ANÁLISIS DE EMBUDO*
📅 Período: {start_date} a {end_date}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 *EMBUDO DE CONVERSIÓN*

1️⃣ SHOPIFY
   └── {shopify_orders} pedidos creados
       │
       ├── ❌ {cancelled_in_shopify} cancelados ({(cancelled_in_shopify/shopify_orders*100) if shopify_orders > 0 else 0:.1f}%)
       │
       ▼
2️⃣ DROPI  
   └── {dropi_total} enviados a proceso
       │
       ├── ✅ {delivered} entregados ({dropi_to_delivered:.1f}%)
       ├── 🔄 {returned} devueltos ({return_rate:.1f}%)
       ├── 🚚 {in_transit} en tránsito
       └── ⏳ {pending} pendientes

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 *TASAS DE CONVERSIÓN*
• Shopify → Dropi: {shopify_to_dropi:.1f}%
• Dropi → Entregado: {dropi_to_delivered:.1f}%
• Tasa de devolución: {return_rate:.1f}%

🎯 *CONVERSIÓN TOTAL*
• De {shopify_orders} pedidos → {delivered} entregas exitosas
• Efectividad: {(delivered/shopify_orders*100) if shopify_orders > 0 else 0:.1f}%

━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""
    
    return report

async def get_leakage_report(args: dict) -> str:
    """Identifica fugas de dinero."""
    print(f"📊 ANALYTICS get_leakage_report - Args: {args}")
    
    if "arguments" in args:
        args = args["arguments"]
    
    start_date = args.get("start_date")
    end_date = args.get("end_date")
    
    if not start_date or not end_date:
        return "❌ Error: Se requieren start_date y end_date"
    
    # Obtener datos
    shopify_response, dropi_response, meta_response = await asyncio.gather(
        get_shopify_orders(start_date, end_date),
        get_dropi_orders(start_date, end_date),
        get_meta_spend(start_date, end_date)
    )
    
    shopify_stats = extract_shopify_stats(shopify_response)
    dropi_stats = extract_dropi_stats(dropi_response)
    meta_spend = extract_spend_from_meta_response(meta_response)
    
    shopify_orders = shopify_stats.get("total_orders", 0)
    dropi_total = dropi_stats.get("total_orders", 0)
    delivered = dropi_stats.get("delivered", 0)
    returned = dropi_stats.get("returned", 0)
    
    # Calcular fugas
    cancelled = shopify_orders - dropi_total
    
    # CPA por pedido
    cpa_per_order = meta_spend / shopify_orders if shopify_orders > 0 else 0
    
    # Fuga 1: Cancelaciones (ads gastados, nunca procesados)
    leak_cancellations = cancelled * cpa_per_order
    
    # Fuga 2: Devoluciones (ads + envío + costo devolución, sin ingreso)
    leak_returns = returned * (cpa_per_order + COSTO_ENVIO + COSTO_DEVOLUCION)
    
    # Total fugado
    total_leaked = leak_cancellations + leak_returns
    
    # Porcentaje del gasto total
    total_investment = meta_spend + (delivered + returned) * (COSTO_PRODUCTO + COSTO_ENVIO)
    leak_percentage = (total_leaked / total_investment * 100) if total_investment > 0 else 0
    
    report = f"""🕳️ *REPORTE DE FUGAS*
📅 Período: {start_date} a {end_date}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💸 *INVERSIÓN TOTAL*
• Gasto en ads: Q{meta_spend:,.2f}
• CPA por pedido: Q{cpa_per_order:.2f}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🚫 *FUGA #1: CANCELACIONES*
• Pedidos cancelados: {cancelled}
• Dinero perdido: Q{leak_cancellations:,.2f}
  (CPA pagado pero nunca procesado)

📦 *FUGA #2: DEVOLUCIONES*
• Pedidos devueltos: {returned}
• Dinero perdido: Q{leak_returns:,.2f}
  (CPA + envío + costo devolución)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💀 *TOTAL FUGADO*
• Monto: Q{total_leaked:,.2f}
• % de inversión: {leak_percentage:.1f}%

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 *RECOMENDACIONES*
"""
    
    if cancelled > shopify_orders * 0.1:
        report += "• ⚠️ Alta tasa de cancelación - revisar proceso de confirmación\n"
    
    if returned > dropi_total * 0.2:
        report += "• ⚠️ Alta tasa de devolución - revisar calidad del targeting\n"
    
    if leak_percentage > 20:
        report += "• 🔴 Fugas críticas - más del 20% se pierde\n"
    elif leak_percentage > 10:
        report += "• 🟡 Fugas moderadas - optimizar para reducir\n"
    else:
        report += "• 🟢 Fugas controladas - buen rendimiento\n"
    
    return report

async def compare_periods(args: dict) -> str:
    """Compara dos períodos."""
    print(f"📊 ANALYTICS compare_periods - Args: {args}")
    
    if "arguments" in args:
        args = args["arguments"]
    
    p1_start = args.get("period1_start")
    p1_end = args.get("period1_end")
    p2_start = args.get("period2_start")
    p2_end = args.get("period2_end")
    
    if not all([p1_start, p1_end, p2_start, p2_end]):
        return "❌ Error: Se requieren las 4 fechas para comparar"
    
    # Obtener datos de ambos períodos en paralelo
    p1_shopify, p1_dropi, p1_meta = await asyncio.gather(
        get_shopify_orders(p1_start, p1_end),
        get_dropi_orders(p1_start, p1_end),
        get_meta_spend(p1_start, p1_end)
    )
    
    p2_shopify, p2_dropi, p2_meta = await asyncio.gather(
        get_shopify_orders(p2_start, p2_end),
        get_dropi_orders(p2_start, p2_end),
        get_meta_spend(p2_start, p2_end)
    )
    
    # Parsear período 1
    p1_stats = {
        "orders": extract_shopify_stats(p1_shopify).get("total_orders", 0),
        "delivered": extract_dropi_stats(p1_dropi).get("delivered", 0),
        "spend": extract_spend_from_meta_response(p1_meta)
    }
    
    # Parsear período 2
    p2_stats = {
        "orders": extract_shopify_stats(p2_shopify).get("total_orders", 0),
        "delivered": extract_dropi_stats(p2_dropi).get("delivered", 0),
        "spend": extract_spend_from_meta_response(p2_meta)
    }
    
    # Calcular CPAs
    p1_cpa = p1_stats["spend"] / p1_stats["delivered"] if p1_stats["delivered"] > 0 else 0
    p2_cpa = p2_stats["spend"] / p2_stats["delivered"] if p2_stats["delivered"] > 0 else 0
    
    # Calcular cambios
    def calc_change(new, old):
        if old == 0:
            return 0
        return ((new - old) / old) * 100
    
    orders_change = calc_change(p2_stats["orders"], p1_stats["orders"])
    delivered_change = calc_change(p2_stats["delivered"], p1_stats["delivered"])
    spend_change = calc_change(p2_stats["spend"], p1_stats["spend"])
    cpa_change = calc_change(p2_cpa, p1_cpa)
    
    def format_change(val):
        if val > 0:
            return f"📈 +{val:.1f}%"
        elif val < 0:
            return f"📉 {val:.1f}%"
        else:
            return "➡️ 0%"
    
    report = f"""📊 *COMPARATIVA DE PERÍODOS*

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📅 *PERÍODO 1: {p1_start} a {p1_end}*
• Pedidos: {p1_stats['orders']}
• Entregados: {p1_stats['delivered']}
• Gasto ads: Q{p1_stats['spend']:,.2f}
• CPA Real: Q{p1_cpa:.2f}

📅 *PERÍODO 2: {p2_start} a {p2_end}*
• Pedidos: {p2_stats['orders']}
• Entregados: {p2_stats['delivered']}
• Gasto ads: Q{p2_stats['spend']:,.2f}
• CPA Real: Q{p2_cpa:.2f}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 *CAMBIOS*
• Pedidos: {format_change(orders_change)}
• Entregas: {format_change(delivered_change)}
• Gasto: {format_change(spend_change)}
• CPA: {format_change(cpa_change)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""
    
    # Análisis
    if cpa_change < 0 and delivered_change > 0:
        report += "\n\n💚 *Excelente:* Más entregas con menor CPA"
    elif cpa_change > 0 and delivered_change < 0:
        report += "\n\n🔴 *Alerta:* Menos entregas con mayor CPA"
    
    return report

# ========== DISPATCHER ==========

TOOL_HANDLERS = {
    "get_profitability_report": get_profitability_report,
    "get_funnel_analysis": get_funnel_analysis,
    "get_leakage_report": get_leakage_report,
    "compare_periods": compare_periods,
}

async def execute_tool(name: str, args: dict) -> str:
    handler = TOOL_HANDLERS.get(name)
    if handler:
        try:
            return await handler(args)
        except Exception as e:
            import traceback
            print(f"❌ Error en {name}: {traceback.format_exc()}")
            return f"Error: {str(e)}"
    return f"Herramienta {name} no encontrada"

# ========== ENDPOINTS HTTP ==========

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
    async def event_generator():
        try:
            yield {"event": "endpoint", "data": f"/messages/{session_id}"}
            while True:
                data = await queue.get()
                yield {"event": "message", "data": json.dumps(data)}
        except asyncio.CancelledError:
            pass
        finally:
            sessions.pop(session_id, None)
    return EventSourceResponse(event_generator())

async def messages_endpoint(request):
    session_id = request.path_params["session_id"]
    if session_id not in sessions:
        return Response("Session not found", status_code=404)
    body = await request.json()
    method = body.get("method", "")
    msg_id = body.get("id")
    if method == "initialize":
        response = {"jsonrpc": "2.0", "id": msg_id, "result": {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "analytics-mcp", "version": "1.0.0"}}}
    elif method == "tools/list":
        response = {"jsonrpc": "2.0", "id": msg_id, "result": {"tools": TOOLS}}
    elif method == "tools/call":
        params = body.get("params", {})
        result = await execute_tool(params.get("name", ""), params.get("arguments", {}))
        response = {"jsonrpc": "2.0", "id": msg_id, "result": {"content": [{"type": "text", "text": result}]}}
    else:
        response = {"jsonrpc": "2.0", "id": msg_id, "result": {}}
    if response and msg_id:
        await sessions[session_id].put(response)
    return Response("OK")

async def health(request):
    """Health check que también verifica conexión a otros servicios."""
    status = {"status": "ok", "services": {}}
    
    async with httpx.AsyncClient(timeout=5.0) as client:
        for name, url in [("dropi", DROPI_URL), ("shopify", SHOPIFY_URL), ("meta", META_URL)]:
            try:
                resp = await client.get(f"{url}/health")
                status["services"][name] = "ok" if resp.status_code == 200 else "error"
            except:
                status["services"][name] = "unreachable"
    
    return JSONResponse(status)

app = Starlette(routes=[
    Route("/", health),
    Route("/health", health),
    Route("/tools", http_tools),
    Route("/call", http_call_tool, methods=["POST"]),
    Route("/sse", sse_endpoint),
    Route("/messages/{session_id}", messages_endpoint, methods=["POST"]),
])

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    print(f"🚀 Analytics Server iniciando en puerto {port}")
    print(f"📡 Conectando a:")
    print(f"   - Dropi: {DROPI_URL}")
    print(f"   - Shopify: {SHOPIFY_URL}")
    print(f"   - Meta: {META_URL}")
    if TIKTOK_URL:
        print(f"   - TikTok: {TIKTOK_URL}")
    uvicorn.run(app, host="0.0.0.0", port=port)
