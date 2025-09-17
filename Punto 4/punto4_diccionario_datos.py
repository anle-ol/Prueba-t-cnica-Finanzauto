
"""
PUNTO 4: Diccionario de datos y parámetros para aplicación
Guarda información de la tabla resultante y explica cómo pasarla a una aplicación
"""

import sqlite3
import json
from datetime import datetime

def conectar_bd():
    """Conecta a la base de datos"""
    conn = sqlite3.connect('encuestas_usuarios_simplificada.db')
    conn.row_factory = sqlite3.Row
    return conn

def crear_diccionario_datos():
    """Crea diccionario de datos de la tabla unificada"""
    
    print("PUNTO 4: Creando diccionario de datos")
    print("=" * 50)
    
    try:
        conn = conectar_bd()
        cursor = conn.cursor()
        
        # Verificar si existe la tabla unificada
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='tabla_unificada_2025'
        """)
        
        if not cursor.fetchone():
            print("La tabla 'tabla_unificada_2025' no existe.")
            print("Ejecuta primero el Punto 3 para crear la tabla.")
            return None
        
        # Obtener información de la estructura de la tabla
        cursor.execute("PRAGMA table_info(tabla_unificada_2025)")
        columnas_info = cursor.fetchall()
        
        # Obtener estadísticas de la tabla
        cursor.execute("SELECT COUNT(*) FROM tabla_unificada_2025")
        total_registros = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT usuario_id) as usuarios_unicos,
                COUNT(DISTINCT id_cuestionario) as tipos_cuestionario,
                AVG(calificacion) as calificacion_promedio,
                MIN(fecha_insercion) as fecha_minima,
                MAX(fecha_insercion) as fecha_maxima
            FROM tabla_unificada_2025
        """)
        estadisticas = cursor.fetchone()
        
        # Crear diccionario de datos
        diccionario_datos = {
            "metadatos": {
                "nombre_tabla": "tabla_unificada_2025",
                "descripcion": "Tabla unificada con información de encuestas y usuarios para Junio-Agosto 2025",
                "fecha_creacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_registros": total_registros,
                "estadisticas": {
                    "usuarios_unicos": estadisticas[0],
                    "tipos_cuestionario": estadisticas[1],
                    "calificacion_promedio": round(estadisticas[2], 2) if estadisticas[2] else 0,
                    "fecha_minima": estadisticas[3],
                    "fecha_maxima": estadisticas[4]
                }
            },
            "estructura_columnas": {},
            "ejemplos_datos": []
        }
        
        # Procesar información de columnas
        for columna in columnas_info:
            col_name = columna[1]
            col_type = columna[2]
            col_notnull = bool(columna[3])
            col_default = columna[4]
            col_pk = bool(columna[5])
            
            # Obtener valores únicos para columnas categóricas
            valores_unicos = None
            if col_type == 'TEXT' and col_name in ['estado', 'descripcion_calificacion']:
                cursor.execute(f"SELECT DISTINCT {col_name} FROM tabla_unificada_2025 WHERE {col_name} IS NOT NULL LIMIT 10")
                valores_unicos = [row[0] for row in cursor.fetchall()]
            
            diccionario_datos["estructura_columnas"][col_name] = {
                "tipo_dato": col_type,
                "descripcion": obtener_descripcion_columna(col_name),
                "permite_nulos": not col_notnull,
                "valor_por_defecto": col_default,
                "es_clave_primaria": col_pk,
                "valores_unicos_ejemplo": valores_unicos
            }
        
        # Obtener muestra de datos
        cursor.execute("SELECT * FROM tabla_unificada_2025 LIMIT 3")
        for row in cursor.fetchall():
            registro = {}
            for i, columna in enumerate(columnas_info):
                registro[columna[1]] = row[i]
            diccionario_datos["ejemplos_datos"].append(registro)
        
        # Guardar diccionario en archivo JSON
        with open('diccionario_datos_tabla_unificada.json', 'w', encoding='utf-8') as f:
            json.dump(diccionario_datos, f, ensure_ascii=False, indent=2)
        
        print("Diccionario de datos creado exitosamente")
        print(f"Archivo guardado: diccionario_datos_tabla_unificada.json")
        print(f"Total registros: {total_registros:,}")
        print(f"Usuarios únicos: {estadisticas[0]:,}")
        print(f"Tipos cuestionario: {estadisticas[1]}")
        print(f"Calificación promedio: {estadisticas[2]:.2f}")
        
        return diccionario_datos
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return None
    
    finally:
        if 'conn' in locals():
            conn.close()

def obtener_descripcion_columna(nombre_columna):
    """Obtiene descripción de cada columna"""
    descripciones = {
        'id_encuesta': 'Identificador único de la encuesta (AUTOINCREMENT)',
        'id_estado_encuesta': 'ID del estado original de la encuesta',
        'estado': 'Estado actual de la encuesta (ej: Completada, Pendiente)',
        'id_cuestionario': 'Identificador del tipo de cuestionario',
        'descripcion_cuestionario': 'Descripción del tipo de cuestionario',
        'calificacion': 'Calificación numérica de la encuesta (1-5)',
        'descripcion_calificacion': 'Descripción textual de la calificación',
        'fecha_limite': 'Fecha límite para completar la encuesta',
        'fecha_creado': 'Fecha de creación del registro',
        'hora_creado': 'Hora de creación del registro',
        'fecha_modificado': 'Fecha de última modificación',
        'hora_modificado': 'Hora de última modificación',
        'fecha_insercion': 'Fecha de inserción en la base de datos',
        'usuario_id': 'Identificador del usuario que respondió',
        'nombre_usuario': 'Nombre del usuario',
        'telefono': 'Número de teléfono del usuario',
        'email': 'Correo electrónico del usuario',
        'año': 'Año extraído de fecha_insercion',
        'mes': 'Mes extraído de fecha_insercion',
        'año_mes': 'Año-mes en formato YYYY-MM'
    }
    return descripciones.get(nombre_columna, 'Columna sin descripción definida')

def main():
    """Función principal"""
    # Crear diccionario de datos
    diccionario = crear_diccionario_datos()
     
    print(f"\n" + "="*60)
    print("PUNTO 4 COMPLETADO")
    print("="*60)
    print("📁 Archivos generados:")
    print("   - diccionario_datos_tabla_unificada.json")
    print("   - punto4_diccionario_datos.py")
    print("\n🎯 RECOMENDACIÓN PRINCIPAL:")
    print("   Para aplicaciones web/API: Usar archivo JSON")
    print("   Para scripts Python: Usar diccionario directo")
    print("   Para funciones específicas: Usar parámetros filtrados")

if __name__ == "__main__":
    main()
