#!/bin/bash

# Script mejorado para ejecutar el sistema de búsqueda de fotos
# Autor: Sistema de Búsqueda de Fotos
# Fecha: $(date)

set -e  # Salir si hay algún error

# Configuración
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/log.log"
PYTHON_SCRIPT="$SCRIPT_DIR/busqueda.py"

# Configuración de Oracle
export ORACLE_HOME=/usr/lib/oracle/21/client64
export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH

# Función para logging
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Función para verificar dependencias
verificar_dependencias() {
    log "Verificando dependencias..."
    
    # Verificar Python
    if ! command -v python3 &> /dev/null; then
        log "ERROR: Python3 no está instalado"
        exit 1
    fi
    
    # Verificar que el script Python existe
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        log "ERROR: No se encuentra el script $PYTHON_SCRIPT"
        exit 1
    fi
    
    # Verificar permisos de escritura
    if [ ! -w "$SCRIPT_DIR" ]; then
        log "ERROR: No hay permisos de escritura en $SCRIPT_DIR"
        exit 1
    fi
    
    log "Dependencias verificadas correctamente"
}

# Función para ejecutar el script
ejecutar_busqueda() {
    log "Iniciando búsqueda de fotos..."
    
    # Cambiar al directorio del script
    cd "$SCRIPT_DIR"
    
    # Ejecutar el script Python
    if python3 "$PYTHON_SCRIPT"; then
        log "Búsqueda completada exitosamente"
    else
        log "ERROR: La búsqueda falló"
        exit 1
    fi
}

# Función para mostrar ayuda
mostrar_ayuda() {
    echo "Uso: $0 [opción]"
    echo ""
    echo "Opciones:"
    echo "  ejecutar    - Ejecutar la búsqueda de fotos (por defecto)"
    echo "  verificar   - Solo verificar dependencias"
    echo "  estadisticas - Mostrar estadísticas del sistema"
    echo "  limpiar     - Limpiar archivos de log"
    echo "  ayuda       - Mostrar esta ayuda"
    echo ""
}

# Función para mostrar estadísticas
mostrar_estadisticas() {
    log "Mostrando estadísticas..."
    python3 "$SCRIPT_DIR/utilidades.py" estadisticas
}

# Función para limpiar logs
limpiar_logs() {
    log "Limpiando logs..."
    python3 "$SCRIPT_DIR/utilidades.py" limpiar
}

# Función para verificar sistema
verificar_sistema() {
    log "Verificando sistema..."
    python3 "$SCRIPT_DIR/utilidades.py" verificar
}

# Procesar argumentos
case "${1:-ejecutar}" in
    "ejecutar")
        verificar_dependencias
        ejecutar_busqueda
        ;;
    "verificar")
        verificar_sistema
        ;;
    "estadisticas")
        mostrar_estadisticas
        ;;
    "limpiar")
        limpiar_logs
        ;;
    "ayuda"|"-h"|"--help")
        mostrar_ayuda
        ;;
    *)
        log "ERROR: Opción desconocida: $1"
        mostrar_ayuda
        exit 1
        ;;
esac