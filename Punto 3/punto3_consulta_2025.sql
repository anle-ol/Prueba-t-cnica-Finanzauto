-- PUNTO 3: Consulta para crear tabla unificada
-- Información de encuestas y usuarios para Junio, Julio y Agosto 2025

-- Crear tabla unificada con información de encuestas y usuarios
CREATE TABLE IF NOT EXISTS tabla_unificada_2025 AS
SELECT 
    e.id_encuesta,
    e.id_estado_encuesta,
    e.estado,
    e.id_cuestionario,
    e.descripcion_cuestionario,
    e.calificacion,
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
LEFT JOIN dimension_calificaciones dc ON e.calificacion = dc.calificacion
WHERE 
    strftime('%Y', e.fecha_insercion) = '2025'
    AND strftime('%m', e.fecha_insercion) IN ('06', '07', '08')
    AND e.calificacion IS NOT NULL
ORDER BY e.fecha_insercion DESC;

-- Verificar la tabla creada
SELECT 
    'Resumen de tabla unificada verano 2025' AS titulo,
    COUNT(*) AS total_registros,
    COUNT(DISTINCT usuario_id) AS usuarios_unicos,
    COUNT(DISTINCT id_cuestionario) AS tipos_cuestionario,
    AVG(calificacion) AS calificacion_promedio,
    MIN(fecha_insercion) AS fecha_mas_antigua,
    MAX(fecha_insercion) AS fecha_mas_reciente
FROM tabla_unificada_2025;

-- Mostrar muestra de datos
SELECT 
    id_encuesta,
    nombre_usuario,
    email,
    estado,
    id_cuestionario,
    calificacion,
    descripcion_calificacion,
    fecha_insercion
FROM tabla_unificada_2025
LIMIT 10;
