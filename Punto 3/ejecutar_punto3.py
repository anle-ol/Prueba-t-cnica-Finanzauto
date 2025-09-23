
"""
PUNTO 3: Ejecutar consulta para tabla unificada 2025
Solo crea la tabla con información de encuestas y usuarios para Junio-Agosto 2025
"""

import sqlite3
import os

def conectar_bd():
    """Conecta a la base de datos"""
    # Usar la base de datos generada en 'Punto 2'
    proyecto_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(proyecto_root, 'Punto 2', 'encuestas_usuarios_simplificada.db')
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA foreign_keys = ON;')
    conn.row_factory = sqlite3.Row
    return conn

def ejecutar_punto3():
    """Ejecuta Punto 3: tabla unificada"""
    
    print("PUNTO 3: Creando tabla unificada")
    print("=" * 50)
    
    try:
        conn = conectar_bd()
        cursor = conn.cursor()
       
        consulta = """
        CREATE TABLE IF NOT EXISTS tabla_unificada_2025 AS
        SELECT 
            e.id_encuesta,
            e.id_estado_encuesta,
            e.estado,
            e.id_cuestionario,
            e.descripcion_cuestionario,
            e.id_calificacion,
            dc.calificacion AS calificacion_valor,
            dc.descripcion AS descripcion_calificacion,
            e.fecha_limite,
            e.fecha_creado,
            e.hora_creado,
            e.fecha_modificado,
            e.hora_modificado,
            e.fecha_insercion,
            e.usuario_id,
            u.nombre AS nombre_usuario,
            u.telefono,
            u.email,
            strftime('%Y', e.fecha_insercion) AS año,
            strftime('%m', e.fecha_insercion) AS mes,
            strftime('%Y-%m', e.fecha_insercion) AS año_mes
        FROM encuestas e
        INNER JOIN usuarios u ON e.usuario_id = u.id_usuario
        LEFT JOIN dimension_calificaciones dc ON e.id_calificacion = dc.id_calificacion
        WHERE 
            strftime('%Y', e.fecha_insercion) = '2025'
            AND strftime('%m', e.fecha_insercion) IN ('06', '07', '08')
            AND e.id_calificacion IS NOT NULL
        ORDER BY e.fecha_insercion DESC
        """
        
        print("Ejecutando consulta...")
        cursor.execute(consulta)
        conn.commit()
        
        cursor.execute("SELECT COUNT(*) FROM tabla_unificada_2025")
        total_registros = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT 
                COUNT(*) AS total_registros,
                COUNT(DISTINCT usuario_id) AS usuarios_unicos,
                AVG(calificacion_valor) AS calificacion_promedio
            FROM tabla_unificada_2025
        """)
        
        resultado = cursor.fetchone()
        
        print(f"\nRESULTADOS:")
        print(f"- Total de registros: {resultado[0]}")
        print(f"- Usuarios únicos: {resultado[1]}")
        print(f"- Calificación promedio: {resultado[2]:.2f}")
        
        # Mostrar muestra
        cursor.execute("""
            SELECT 
                id_encuesta,
                nombre_usuario,
                email,
                calificacion_valor,
                fecha_insercion
            FROM tabla_unificada_2025
            LIMIT 5
        """)
        
        print(f"\nMUESTRA DE DATOS:")
        for row in cursor.fetchall():
            print(f"- ID: {row[0]}, Usuario: {row[1]}, Email: {row[2]}, "
                  f"Calificación: {row[3]}, Fecha: {row[4]}")
        
        print(f"\nTabla 'tabla_unificada_2025' creada exitosamente")
        
    except Exception as e:
        print(f"Error: {str(e)}")
    
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    ejecutar_punto3()
