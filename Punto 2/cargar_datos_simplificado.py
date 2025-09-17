"""
Script SIMPLIFICADO para cargar datos con 3 tablas y ID AUTOINCREMENTAL
SOLUCIÓN: Solo 3 tablas originales, pero con ID único real en ENCUESTAS
"""

import sqlite3
import csv
import os
import hashlib

def conectar_bd():
    """Conecta a la base de datos SQLite"""
    conn = sqlite3.connect('encuestas_usuarios_simplificada.db')
    return conn

def crear_tablas_simplificadas(conn):
    """Crea las 3 tablas con ID AUTOINCREMENTAL en ENCUESTAS"""
    cursor = conn.cursor()
    
    # Tabla usuarios (sin cambios)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id_usuario INTEGER PRIMARY KEY,
            nombre TEXT NOT NULL,
            telefono TEXT,
            email TEXT
        )
    ''')
    
    # Tabla encuestas (CON ID AUTOINCREMENTAL)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS encuestas (
            id_encuesta INTEGER PRIMARY KEY AUTOINCREMENT,
            id_estado_encuesta INTEGER,
            estado TEXT,
            id_cuestionario INTEGER,
            descripcion_cuestionario TEXT,
            calificacion INTEGER,
            fecha_limite TEXT,
            fecha_creado TEXT,
            hora_creado TEXT,
            fecha_modificado TEXT,
            hora_modificado TEXT,
            fecha_insercion TEXT,
            usuario_id INTEGER,
            hash_unico TEXT UNIQUE,
            FOREIGN KEY (usuario_id) REFERENCES usuarios (id_usuario)
        )
    ''')
    
    # Tabla dimension_calificaciones (sin cambios)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dimension_calificaciones (
            id_calificacion INTEGER PRIMARY KEY,
            calificacion INTEGER,
            descripcion TEXT
        )
    ''')
    
    conn.commit()
    print("3 tablas creadas exitosamente")
    print("   - usuarios")
    print("   - encuestas (CON ID AUTOINCREMENTAL)")
    print("   - dimension_calificaciones")

def generar_hash_unico(row):
    """Genera un hash único para identificar encuestas duplicadas"""
    campos_clave = f"{row.get('usuario_id', '')}-{row.get('Fecha_Insercion', '')}-{row.get('IdCuestionario', '')}-{row.get('Calificacion', '')}"
    return hashlib.md5(campos_clave.encode()).hexdigest()

def cargar_usuarios(conn):
    """Carga los datos de usuarios desde el CSV"""
    print("📂 Cargando datos de usuarios...")
    
    cursor = conn.cursor()
    cursor.execute("DELETE FROM usuarios")
    
    with open('usuarios.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')
        count = 0
        for row in reader:
            if row.get('id_usuario') and row['id_usuario'].strip():
                cursor.execute('''
                    INSERT INTO usuarios (id_usuario, nombre, telefono, email)
                    VALUES (?, ?, ?, ?)
                ''', (
                    int(row['id_usuario']), 
                    row.get('nombre', ''), 
                    row.get('telefono', ''), 
                    row.get('email', '')
                ))
                count += 1
    
    conn.commit()
    print(f"{count} usuarios cargados")

def cargar_encuestas_simplificadas(conn):
    """Carga los datos de encuestas"""
    print("Cargando datos de encuestas ...")
    
    cursor = conn.cursor()
    cursor.execute("DELETE FROM encuestas")
    
    encuestas_procesadas = 0
    encuestas_duplicadas = 0
    encuestas_validas = 0
    
    with open('Encuestas.csv', 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file, delimiter=';')
        
        for row in reader:
            if row.get('usuario_id') and row['usuario_id'].strip():
                # Generar hash único para detectar duplicados
                hash_unico = generar_hash_unico(row)
                
                # Verificar si ya existe esta encuesta
                cursor.execute("SELECT COUNT(*) FROM encuestas WHERE hash_unico = ?", (hash_unico,))
                if cursor.fetchone()[0] > 0:
                    encuestas_duplicadas += 1
                    continue
                
                # Limpiar campos numéricos
                calificacion = None
                if row.get('Calificacion') and row['Calificacion'] != 'NULL':
                    try:
                        calificacion = int(row['Calificacion'])
                    except:
                        calificacion = None
                
                id_cuestionario = None
                if row.get('IdCuestionario') and row['IdCuestionario'] != 'NULL':
                    try:
                        id_cuestionario = int(row['IdCuestionario'])
                    except:
                        id_cuestionario = None
                
                id_estado = None
                if row.get('IdEstadoEncuesta'):
                    try:
                        id_estado = int(row['IdEstadoEncuesta'])
                    except:
                        id_estado = None
                
                cursor.execute('''
                    INSERT INTO encuestas (
                        id_estado_encuesta, estado, id_cuestionario, descripcion_cuestionario,
                        calificacion, fecha_limite, fecha_creado, hora_creado,
                        fecha_modificado, hora_modificado, fecha_insercion,
                        usuario_id, hash_unico
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    id_estado,
                    row.get('Estado', ''),
                    id_cuestionario,
                    row.get('DescripcionCuestionario', ''),
                    calificacion,
                    row.get('FechaLimite', ''),
                    row.get('FechaCreado', ''),
                    row.get('HoraCreado', ''),
                    row.get('FechaModificado', ''),
                    row.get('HoraModificado', ''),
                    row.get('Fecha_Insercion', ''),
                    int(row['usuario_id']),
                    hash_unico
                ))
                
                encuestas_procesadas += 1
                if calificacion is not None:
                    encuestas_validas += 1
                
            
                if encuestas_procesadas >= 30000:
                    print(f"Procesando solo los primeros 10,000 registros para prueba")
                    break
    
    conn.commit()
    print(f"{encuestas_procesadas} encuestas cargadas")
    print(f"   - Encuestas válidas (con calificación): {encuestas_validas}")
    print(f"   - Encuestas duplicadas omitidas: {encuestas_duplicadas}")

def cargar_dimension_calificaciones(conn):
    """Carga la dimensión de calificaciones"""
    print("Cargando dimensión de calificaciones...")
    
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dimension_calificaciones")
    
    with open('dimension_calificaciones.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')
        count = 0
        for row in reader:
            cursor.execute('''
                INSERT INTO dimension_calificaciones (id_calificacion, calificacion, descripcion)
                VALUES (?, ?, ?)
            ''', (
                int(row['id_calificacion']), 
                int(row['calificacion']), 
                row['descripcion']
            ))
            count += 1
    
    conn.commit()
    print(f" {count} calificaciones cargadas")

def verificar_datos_simplificados(conn):
    """Verifica que los datos simplificados se cargaron correctamente"""
    cursor = conn.cursor()
    
    # Contar registros por tabla
    tablas = ['usuarios', 'encuestas', 'dimension_calificaciones']
    print("\nResumen de datos simplificados:")
    
    for tabla in tablas:
        cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
        count = cursor.fetchone()[0]
        print(f"   - {tabla}: {count}")
    
    # Mostrar estructura de encuestas con ID AUTOINCREMENTAL
    print("\nMuestra de encuestas:")
    cursor.execute('''
        SELECT 
            e.id_encuesta,
            e.id_estado_encuesta,
            e.estado,
            e.calificacion,
            u.nombre,
            e.fecha_insercion
        FROM encuestas e
        LEFT JOIN usuarios u ON e.usuario_id = u.id_usuario
        WHERE e.calificacion IS NOT NULL
        LIMIT 5
    ''')
    
    for row in cursor.fetchall():
        print(f"   ID: {row[0]}, Estado ID: {row[1]}, Estado: {row[2]}, "
              f"Calificación: {row[3]}, Usuario: {row[4]}, Fecha: {row[5]}")
    
    # Verificar que los IDs son únicos y secuenciales
    print("\nVerificación de IDs")
    cursor.execute('''
        SELECT 
            MIN(id_encuesta) as min_id,
            MAX(id_encuesta) as max_id,
            COUNT(*) as total_encuestas,
            COUNT(DISTINCT id_encuesta) as ids_unicos
        FROM encuestas
    ''')
    
    resultado = cursor.fetchone()
    print(f"   - ID mínimo: {resultado[0]}")
    print(f"   - ID máximo: {resultado[1]}")
    print(f"   - Total encuestas: {resultado[2]}")
    print(f"   - IDs únicos: {resultado[3]}")
    
    if resultado[2] == resultado[3]:
        print("   Todos los IDs son únicos")
    else:
        print("   Hay IDs duplicados")

def main():
    """Función principal simplificada"""
    print("Iniciando carga de datos SIMPLIFICADA...")
    print("=" * 60)
    print("SOLUCIÓN: ENCUESTAS")
    print("=" * 60)
    
    try:
        # Conectar a la base de datos
        conn = conectar_bd()
        print("Conexión a base de datos establecida")
        
        # Crear tablas simplificadas
        crear_tablas_simplificadas(conn)
        
        # Cargar datos
        cargar_usuarios(conn)
        cargar_encuestas_simplificadas(conn)
        cargar_dimension_calificaciones(conn)
        
        # Verificar datos simplificados
        verificar_datos_simplificados(conn)
        
        print("\n¡Carga de datos completada")
        print(" Base de datos: encuestas_usuarios_simplificada.db")
        print("\nCARACTERÍSTICAS DE LA SOLUCIÓN :")
        print("   ID AUTOINCREMENTAL en tabla ENCUESTAS")
        print("   ID único real para cada encuesta")
        print("   Estructura simple y mantenible")
        print("   Detección y eliminación de duplicados")
        
    except Exception as e:
        print(f"Error durante la carga: {str(e)}")
        import traceback
        traceback.print_exc()
    

if __name__ == "__main__":
    main()
