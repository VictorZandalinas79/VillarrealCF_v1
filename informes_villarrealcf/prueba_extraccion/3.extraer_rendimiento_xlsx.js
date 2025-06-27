const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');
const parquet = require('parquetjs');

// Configuración
const BASE_PATH = 'VCF_Mediacoach_Data/Temporada_24_25/La_Liga/Partidos';
const OUTPUT_BASE_PATH = 'data';

// Mapping de nombres de hoja a nombres de archivo
const SHEET_TO_FILE_MAP = {
    'Físico': 'rendimiento_fisico.parquet',
    'Fisico': 'rendimiento_fisico.parquet', // Por si no tiene tilde
    '5': 'rendimiento_5.parquet',
    '10': 'rendimiento_10.parquet',
    '15': 'rendimiento_15.parquet'
};

function obtenerNombreArchivoParquet(nombreHoja) {
    // Normalizar el nombre de la hoja (quitar espacios, convertir a lowercase para comparar)
    const nombreNormalizado = nombreHoja.trim();
    
    // Buscar coincidencia exacta primero
    if (SHEET_TO_FILE_MAP[nombreNormalizado]) {
        return SHEET_TO_FILE_MAP[nombreNormalizado];
    }
    
    // Buscar coincidencia insensitive a mayúsculas/minúsculas
    const nombreLower = nombreNormalizado.toLowerCase();
    for (const [sheetName, fileName] of Object.entries(SHEET_TO_FILE_MAP)) {
        if (sheetName.toLowerCase() === nombreLower) {
            return fileName;
        }
    }
    
    // Si no encuentra coincidencia, generar nombre genérico
    const nombreLimpio = nombreNormalizado.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
    return `rendimiento_${nombreLimpio}.parquet`;
}

async function leerDatosParquetExistentes(rutaArchivo) {
    try {
        if (fs.existsSync(rutaArchivo)) {
            console.log(`📖 Cargando datos existentes de ${path.basename(rutaArchivo)}...`);
            const reader = await parquet.ParquetReader.openFile(rutaArchivo);
            const cursor = reader.getCursor();
            const datos = [];
            
            let record = null;
            while (record = await cursor.next()) {
                datos.push(record);
            }
            
            await reader.close();
            console.log(`✅ Cargadas ${datos.length} filas existentes de ${path.basename(rutaArchivo)}`);
            return datos;
        } else {
            console.log(`🆕 No existe archivo ${path.basename(rutaArchivo)}, creando nuevo...`);
            return [];
        }
    } catch (error) {
        console.log(`⚠️ Error leyendo ${path.basename(rutaArchivo)}: ${error.message}`);
        console.log('🆕 Iniciando con datos vacíos...');
        return [];
    }
}

function generarClaveUnica(fila) {
    // Genera una clave única para detectar duplicados
    const campos = [
        fila['Id Jugador'] || fila['id_jugador'] || '',
        fila.Competicion || fila.liga || '',
        fila.Temporada || fila.temporada || '',
        fila.Jornada || fila.jornada || '',
        fila.Partido || fila.partido || '',
        fila.Equipo || fila.equipo || '',
        fila.archivo_origen || '',
        fila.tipo_reporte || '',
        fila.hoja || '' // Agregamos la hoja para diferenciar
    ];
    
    return campos.join('|').toLowerCase().trim();
}

function crearConjuntoDuplicados(datosExistentes) {
    const clavesExistentes = new Set();
    
    datosExistentes.forEach(fila => {
        const clave = generarClaveUnica(fila);
        if (clave && clave !== '||||||||') {
            clavesExistentes.add(clave);
        }
    });
    
    console.log(`🔍 Creado índice de ${clavesExistentes.size} registros únicos existentes`);
    return clavesExistentes;
}

function filtrarDatosDuplicados(datosNuevos, clavesExistentes) {
    const datosFiltrados = [];
    const duplicadosEncontrados = [];
    const duplicadosInternos = new Set();
    
    datosNuevos.forEach((fila, index) => {
        const clave = generarClaveUnica(fila);
        
        if (!clave || clave === '||||||||') {
            console.log(`⚠️ Fila ${index + 1} tiene datos incompletos, se omite`);
            return;
        }
        
        if (clavesExistentes.has(clave)) {
            duplicadosEncontrados.push({
                fila: index + 1,
                jugador: fila['Id Jugador'] || 'Sin ID',
                partido: `${fila.Jornada || fila.jornada} | ${fila.Partido || fila.partido}`,
                equipo: fila.Equipo || fila.equipo,
                tipo: fila.tipo_reporte,
                hoja: fila.hoja
            });
            return;
        }
        
        if (duplicadosInternos.has(clave)) {
            console.log(`🔄 Duplicado interno detectado en fila ${index + 1}`);
            return;
        }
        
        duplicadosInternos.add(clave);
        datosFiltrados.push(fila);
    });
    
    if (duplicadosEncontrados.length > 0) {
        console.log(`\n🚫 Duplicados detectados (NO se añadirán): ${duplicadosEncontrados.length}`);
    }
    
    return {
        datosFiltrados,
        totalDuplicados: duplicadosEncontrados.length,
        duplicadosInternos: datosNuevos.length - datosFiltrados.length - duplicadosEncontrados.length
    };
}

function extraerJornadaPartido(nombreCarpeta) {
    let jornada = null;
    let partido = null;
    
    // Extraer jornada (si empieza por j seguido de número)
    if (nombreCarpeta.toLowerCase().startsWith('j') && nombreCarpeta.length > 1) {
        for (let i = 1; i < nombreCarpeta.length; i++) {
            if (!nombreCarpeta[i].match(/\d/)) {
                jornada = nombreCarpeta.substring(0, i);
                break;
            }
        }
        if (!jornada) jornada = nombreCarpeta;
    }
    
    // Extraer partido (desde la primera _ hasta el final)
    if (nombreCarpeta.includes('_')) {
        partido = nombreCarpeta.substring(nombreCarpeta.indexOf('_') + 1);
    }
    
    return { jornada, partido };
}

function extraerEquipo(sheet) {
    const range = XLSX.utils.decode_range(sheet['!ref']);
    console.log(`    🔍 Buscando equipo en rango ${sheet['!ref']}`);

    // Expandir área de búsqueda y añadir más logging
    for (let row = 0; row <= Math.min(50, range.e.r); row++) {
        for (let col = 0; col <= Math.min(30, range.e.c); col++) {
            const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
            const cell = sheet[cellAddress];

            if (cell && typeof cell.v === 'string') {
                const valorOriginal = cell.v.trim();
                const valor = valorOriginal.toLowerCase();

                // Log de celdas que contienen "informe" para debugging
                if (valor.includes('informe')) {
                    console.log(`    📍 Celda ${cellAddress}: "${valorOriginal}"`);
                }

                // Patrones específicos para capturar los diferentes formatos encontrados
                const patrones = [
                    // Patrón 1: "Informe de Rendimiento Físico Intervalos X' EQUIPO"
                    /informe\s+de\s+rendimiento\s+físico\s+intervalos\s+\d+['´]\s+(.+)/i,
                    // Patrón 2: "Informe de Rendimiento Físico EQUIPO" (sin intervalos)
                    /informe\s+de\s+rendimiento\s+físico\s+(?!intervalos)(.+)/i,
                    // Patrón 3: "Informe de Rendimiento Intervalos X' EQUIPO" (sin "Físico")
                    /informe\s+de\s+rendimiento\s+intervalos\s+\d+['´]\s+(.+)/i,
                    // Patrón 4: "Informe de Rendimiento EQUIPO" (sin "Físico" ni "Intervalos")
                    /informe\s+de\s+rendimiento\s+(?!físico|intervalos)(.+)/i,
                    // Patrón 5: Más general como fallback
                    /informe\s+de\s+rendimiento\s+(.+)/i
                ];

                for (const patron of patrones) {
                    const match = valorOriginal.match(patron);
                    if (match && match[1]) {
                        let equipo = match[1].trim();
                        
                        // Limpiar posibles sufijos no deseados más específicamente
                        equipo = equipo.replace(/\s*(vs\.?|contra|v\.?)\s+.*/i, ''); // Quitar "vs Equipo2"
                        equipo = equipo.replace(/\s*-\s*.*/i, ''); // Quitar texto después de guión
                        equipo = equipo.replace(/\s*\(.*\)/g, ''); // Quitar texto entre paréntesis
                        equipo = equipo.replace(/\s*\[.*\]/g, ''); // Quitar texto entre corchetes
                        equipo = equipo.replace(/\s+(jornada|partido|fecha|temporada)\s+.*/i, ''); // Quitar info del partido
                        
                        // Validar que es un nombre de equipo razonable
                        if (equipo.length > 2 && equipo.length < 50 && !equipo.match(/^\d+$/)) {
                            console.log(`    ✅ Equipo encontrado en ${cellAddress}: "${equipo}" (patrón usado: ${patron.toString()})`);
                            return equipo;
                        } else {
                            console.log(`    ⚠️ Descartado "${equipo}" - no parece un nombre válido`);
                        }
                    }
                }

                // Patrón adicional: buscar líneas que contengan nombres de equipos conocidos
                const equiposConocidos = [
                    'barcelona', 'real madrid', 'atletico', 'sevilla', 'valencia', 'villarreal',
                    'real sociedad', 'athletic', 'betis', 'girona', 'getafe', 'osasuna',
                    'rayo vallecano', 'celta', 'mallorca', 'las palmas', 'cadiz', 'espanyol',
                    'valladolid', 'almeria', 'elche'
                ];

                if (valor.includes('informe')) {
                    for (const equipoConocido of equiposConocidos) {
                        if (valor.includes(equipoConocido)) {
                            // Intentar extraer el nombre completo del equipo
                            const words = valorOriginal.split(/\s+/);
                            const equipoIndex = words.findIndex(word => 
                                word.toLowerCase().includes(equipoConocido)
                            );
                            
                            if (equipoIndex !== -1) {
                                // Tomar desde el nombre del equipo hasta el final (o hasta 3 palabras)
                                const equipoCompleto = words.slice(equipoIndex, equipoIndex + 3).join(' ');
                                console.log(`    ✅ Equipo encontrado por coincidencia: "${equipoCompleto}"`);
                                return equipoCompleto;
                            }
                        }
                    }
                }
            }
        }
    }

    // Si no encuentra nada, intentar buscar en toda la hoja palabras clave
    console.log(`    ⚠️ No se encontró "Informe de Rendimiento" - buscando alternativamente...`);
    
    for (let row = 0; row <= Math.min(50, range.e.r); row++) {
        for (let col = 0; col <= Math.min(30, range.e.c); col++) {
            const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
            const cell = sheet[cellAddress];

            if (cell && typeof cell.v === 'string') {
                const valor = cell.v.trim();
                
                // Buscar patrones alternativos
                if (valor.match(/^(FC|CF|Real|Athletic|Rayo|UD)\s+\w+/i) ||
                    valor.match(/\w+\s+(FC|CF|UD)$/i)) {
                    console.log(`    🔍 Posible equipo alternativo en ${cellAddress}: "${valor}"`);
                    return valor;
                }
            }
        }
    }

    console.log(`    ❌ No se pudo extraer el equipo de la hoja`);
    return null;
}

function encontrarFilaHeaders(sheet) {
    const range = XLSX.utils.decode_range(sheet['!ref']);
    
    for (let row = 0; row <= Math.min(20, range.e.r); row++) {
        for (let col = 0; col <= Math.min(10, range.e.c); col++) {
            const cellAddress = XLSX.utils.encode_cell({r: row, c: col});
            const cell = sheet[cellAddress];
            
            if (cell && cell.v && String(cell.v).trim() === 'Id Jugador') {
                return row;
            }
        }
    }
    return null;
}

// Función para limpiar y estandarizar las columnas
function limpiarColumnasMetadatos(headers) {
    // Columnas de metadatos que pueden venir duplicadas en los Excel originales
    const columnasMetadatosConflictivas = [
        'temporada', 'competicion', 'liga', 'jornada', 'partido', 'equipo',
        'season', 'competition', 'matchday', 'match', 'team'
    ];
    
    // Filtrar headers removiendo las columnas que vamos a añadir nosotros
    const headersLimpios = headers.filter(header => {
        if (!header || header.trim() === '') return false;
        
        const headerNormalizado = header.toLowerCase().trim();
        return !columnasMetadatosConflictivas.includes(headerNormalizado);
    });
    
    console.log(`    🧹 Headers originales: ${headers.length}, después de limpiar: ${headersLimpios.length}`);
    if (headers.length !== headersLimpios.length) {
        const removidos = headers.filter(h => !headersLimpios.includes(h));
        console.log(`    🗑️ Columnas removidas: ${removidos.join(', ')}`);
    }
    
    return headersLimpios;
}

// Función mejorada para añadir metadatos de forma consistente
function añadirMetadatosEstandarizados(obj, equipo, jornada, partido, tipoReporte, nombreHoja, nombreArchivo) {
    // Añadir metadatos con nombres estandarizados AL FINAL
    obj.Temporada = '24_25';
    obj.Competicion = 'La Liga';
    obj.Jornada = jornada;
    obj.Partido = partido;
    obj.Equipo = equipo;
    obj.tipo_reporte = tipoReporte;
    obj.hoja = nombreHoja;
    obj.archivo_origen = nombreArchivo;
    
    return obj;
}

function procesarHojaXlsx(sheet, nombreHoja, jornada, partido, tipoReporte, nombreArchivo) {
    console.log(`    📄 Procesando hoja: ${nombreHoja} (${tipoReporte})`);
    
    try {
        const equipo = extraerEquipo(sheet);
        if (!equipo) {
            console.log(`    ❌ No se encontró el equipo en hoja ${nombreHoja}`);
            console.log(`    🔍 Contenido de las primeras celdas para debugging:`);
            
            // Mostrar contenido de las primeras celdas para debugging
            const range = XLSX.utils.decode_range(sheet['!ref']);
            for (let row = 0; row <= Math.min(10, range.e.r); row++) {
                for (let col = 0; col <= Math.min(10, range.e.c); col++) {
                    const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
                    const cell = sheet[cellAddress];
                    if (cell && cell.v && typeof cell.v === 'string' && cell.v.trim().length > 5) {
                        console.log(`      ${cellAddress}: "${cell.v.trim()}"`);
                    }
                }
            }
            return null;
        }

        const filaHeaders = encontrarFilaHeaders(sheet);
        if (filaHeaders === null) {
            console.log(`    ❌ No se encontró fila con 'Id Jugador' en hoja ${nombreHoja}`);
            return null;
        }
        
        console.log(`    📍 Headers en fila: ${filaHeaders + 1} (hoja: ${nombreHoja})`);
        console.log(`    🏃 Equipo: ${equipo}`);
        
        const jsonData = XLSX.utils.sheet_to_json(sheet, {
            range: filaHeaders,
            header: 1
        });
        
        if (jsonData.length === 0) {
            console.log(`    ❌ No hay datos en hoja ${nombreHoja}`);
            return null;
        }
        
        const headersOriginales = jsonData[0];
        const headersDesdeColumna2 = headersOriginales.slice(1); // Desde columna 2 (sin "Id Jugador")
        
        // 🧹 LIMPIAR COLUMNAS CONFLICTIVAS
        const headersLimpios = limpiarColumnasMetadatos(headersDesdeColumna2);
        
        console.log(`    📊 Headers finales: ${headersLimpios.length} columnas de datos + metadatos`);
        
        const datosProcessed = jsonData.slice(1).map(row => {
            const rowDesdeColumna2 = row.slice(1); // Desde columna 2
            const obj = {};
            
            // Añadir solo los headers limpios (sin metadatos conflictivos)
            headersLimpios.forEach((header, index) => {
                // Buscar el índice original del header limpio en los headers originales desde columna 2
                const indiceOriginal = headersDesdeColumna2.indexOf(header);
                if (indiceOriginal !== -1 && indiceOriginal < rowDesdeColumna2.length) {
                    obj[header || `columna_${index + 2}`] = rowDesdeColumna2[indiceOriginal];
                }
            });
            
            // 📝 AÑADIR METADATOS ESTANDARIZADOS
            return añadirMetadatosEstandarizados(obj, equipo, jornada, partido, tipoReporte, nombreHoja, nombreArchivo);
            
        }).filter(row => {
            // Filtrar filas que tienen al menos algún dato (excluyendo los metadatos)
            const datosOriginales = Object.entries(row).filter(([key, val]) => 
                !['Temporada', 'Competicion', 'Jornada', 'Partido', 'Equipo', 'tipo_reporte', 'hoja', 'archivo_origen'].includes(key)
            );
            return datosOriginales.some(([key, val]) => val !== null && val !== undefined && val !== '');
        });
        
        console.log(`    ✅ Procesadas ${datosProcessed.length} filas de hoja ${nombreHoja}`);
        
        // 🔍 MOSTRAR SAMPLE DE COLUMNAS PARA VERIFICACIÓN
        if (datosProcessed.length > 0) {
            const columnasFinales = Object.keys(datosProcessed[0]);
            console.log(`    📋 Columnas finales (${columnasFinales.length}): ${columnasFinales.join(', ')}`);
            
            // Verificar que no hay duplicados
            const columnasDuplicadas = columnasFinales.filter((col, index) => 
                columnasFinales.indexOf(col) !== index
            );
            if (columnasDuplicadas.length > 0) {
                console.log(`    ⚠️ COLUMNAS DUPLICADAS DETECTADAS: ${columnasDuplicadas.join(', ')}`);
            }
        }
        
        return datosProcessed;
        
    } catch (error) {
        console.log(`    ❌ Error en hoja ${nombreHoja}: ${error.message}`);
        return null;
    }
}

function procesarArchivoXlsx(archivoPath, jornada, partido, tipoReporte) {
    const nombreArchivo = path.basename(archivoPath);
    console.log(`  📄 Procesando: ${nombreArchivo} (${tipoReporte})`);
    
    try {
        const workbook = XLSX.readFile(archivoPath, {
            cellStyles: false,
            cellFormulas: true,   
            cellDates: true,     
            cellNF: false,
            sheetStubs: true     
        });
        
        const resultadosPorHoja = {};
        
        // Procesar cada hoja del workbook
        workbook.SheetNames.forEach(nombreHoja => {
            console.log(`    🔍 Procesando hoja: ${nombreHoja}`);
            const sheet = workbook.Sheets[nombreHoja];
            
            const datosHoja = procesarHojaXlsx(sheet, nombreHoja, jornada, partido, tipoReporte, nombreArchivo);
            
            if (datosHoja && datosHoja.length > 0) {
                resultadosPorHoja[nombreHoja] = datosHoja;
            }
        });
        
        console.log(`    ✅ Procesadas ${Object.keys(resultadosPorHoja).length} hojas exitosamente`);
        return resultadosPorHoja;
        
    } catch (error) {
        console.log(`    ❌ Error: ${error.message}`);
        return null;
    }
}

function buscarArchivosXlsxEnCarpeta(carpetaPath) {
    try {
        const archivos = fs.readdirSync(carpetaPath);
        console.log(`    📁 Archivos en carpeta: ${archivos.join(', ')}`);
        
        // ✅ CORREGIDO: ahora es "rendimiento_1" (con 'i')
        const rendimiento1 = archivos.find(archivo => 
            archivo.toLowerCase().startsWith('rendimiento_1') && archivo.endsWith('.xlsx')
        );
        
        const rendimiento2 = archivos.find(archivo => 
            archivo.toLowerCase().startsWith('rendimiento_2') && archivo.endsWith('.xlsx')
        );
        
        console.log(`    🔍 Archivos encontrados:`);
        console.log(`      📊 rendimiento_1: ${rendimiento1 || 'NO ENCONTRADO'}`);
        console.log(`      📊 rendimiento_2: ${rendimiento2 || 'NO ENCONTRADO'}`);
        
        return {
            rendimiento1: rendimiento1 ? path.join(carpetaPath, rendimiento1) : null,
            rendimiento2: rendimiento2 ? path.join(carpetaPath, rendimiento2) : null
        };
    } catch (error) {
        console.log(`    ❌ Error leyendo carpeta: ${error.message}`);
        return { rendimiento1: null, rendimiento2: null };
    }
}

async function escribirParquet(datos, rutaCompleta) {
    try {
        if (datos.length === 0) {
            console.log('⚠️ No hay datos para escribir');
            return false;
        }

        const primerFilaDatos = datos[0];
        const schemaDef = {};

        Object.keys(primerFilaDatos).forEach(campo => {
            const valorEjemplo = primerFilaDatos[campo];
            let tipo = { type: 'UTF8' };

            if (typeof valorEjemplo === 'number') {
                if (Number.isInteger(valorEjemplo)) {
                    tipo = { type: 'INT64' };
                } else {
                    tipo = { type: 'DOUBLE' };
                }
            } else if (typeof valorEjemplo === 'boolean') {
                tipo = { type: 'BOOLEAN' };
            }

            schemaDef[campo] = tipo;
        });

        const schema = new parquet.ParquetSchema(schemaDef);
        const writer = await parquet.ParquetWriter.openFile(schema, rutaCompleta);

        for (const fila of datos) {
            await writer.appendRow(fila);
        }

        await writer.close();
        return true;
    } catch (error) {
        console.log(`❌ Error escribiendo parquet: ${error.message}`);
        return false;
    }
}

// Función para verificar consistencia de columnas entre hojas
function verificarConsistenciaColumnas(datosPorHoja) {
    console.log('\n🔍 VERIFICANDO CONSISTENCIA DE COLUMNAS ENTRE HOJAS:');
    console.log('=' .repeat(70));
    
    const metadatosEsperados = ['Temporada', 'Competicion', 'Jornada', 'Partido', 'Equipo', 'tipo_reporte', 'hoja', 'archivo_origen'];
    
    Object.entries(datosPorHoja).forEach(([nombreHoja, datos]) => {
        if (datos.length > 0) {
            const columnas = Object.keys(datos[0]);
            const metadatosEncontrados = columnas.filter(col => metadatosEsperados.includes(col));
            const metadatosFaltantes = metadatosEsperados.filter(meta => !columnas.includes(meta));
            
            console.log(`📄 Hoja "${nombreHoja}":`);
            console.log(`  📊 Total columnas: ${columnas.length}`);
            console.log(`  ✅ Metadatos presentes: ${metadatosEncontrados.join(', ')}`);
            if (metadatosFaltantes.length > 0) {
                console.log(`  ❌ Metadatos faltantes: ${metadatosFaltantes.join(', ')}`);
            }
            
            // Buscar posibles duplicados en nombres de columnas
            const columnasDuplicadas = columnas.filter((col, index) => 
                columnas.indexOf(col) !== index
            );
            if (columnasDuplicadas.length > 0) {
                console.log(`  ⚠️ Columnas duplicadas: ${columnasDuplicadas.join(', ')}`);
            }
            
            console.log(`  📋 Primeras 10 columnas: ${columnas.slice(0, 10).join(', ')}${columnas.length > 10 ? '...' : ''}`);
        }
    });
    
    console.log('=' .repeat(70));
}

// Función para diagnosticar nombres de archivos problemáticos
function diagnosticarNombresArchivos(basePath) {
    console.log('\n🔍 DIAGNÓSTICO DE NOMBRES DE ARCHIVOS:');
    console.log('=' .repeat(70));
    
    const carpetas = fs.readdirSync(basePath).filter(item => {
        const carpetaPath = path.join(basePath, item);
        return fs.statSync(carpetaPath).isDirectory();
    });
    
    let carpetasSinArchivos = 0;
    let carpetasConSoloUno = 0;
    let carpetasCompletas = 0;
    
    carpetas.forEach(carpeta => {
        const carpetaPath = path.join(basePath, carpeta);
        try {
            const archivos = fs.readdirSync(carpetaPath).filter(f => f.endsWith('.xlsx'));
            
            const rendimiento1 = archivos.find(a => a.toLowerCase().startsWith('rendimiento_1'));
            const rendimiento2 = archivos.find(a => a.toLowerCase().startsWith('rendimiento_2'));
            
            let status = '';
            if (!rendimiento1 && !rendimiento2) {
                status = '❌ SIN ARCHIVOS';
                carpetasSinArchivos++;
            } else if (rendimiento1 && rendimiento2) {
                status = '✅ COMPLETA';
                carpetasCompletas++;
            } else {
                status = '⚠️ SOLO UNO';
                carpetasConSoloUno++;
            }
            
            console.log(`${status} ${carpeta}:`);
            if (archivos.length > 0) {
                archivos.forEach(archivo => {
                    const esR1 = archivo.toLowerCase().startsWith('rendimiento_1') ? '📊1' : '';
                    const esR2 = archivo.toLowerCase().startsWith('rendimiento_2') ? '📊2' : '';
                    console.log(`    ${esR1}${esR2} ${archivo}`);
                });
            } else {
                console.log(`    (sin archivos .xlsx)`);
            }
        } catch (error) {
            console.log(`❌ ERROR ${carpeta}: ${error.message}`);
        }
    });
    
    console.log('\n📊 RESUMEN DEL DIAGNÓSTICO:');
    console.log(`  ✅ Carpetas completas (2 archivos): ${carpetasCompletas}`);
    console.log(`  ⚠️ Carpetas con solo 1 archivo: ${carpetasConSoloUno}`);
    console.log(`  ❌ Carpetas sin archivos: ${carpetasSinArchivos}`);
    console.log(`  📁 Total carpetas: ${carpetas.length}`);
    console.log(`  📄 Archivos esperados: ${carpetas.length * 2}`);
    console.log('=' .repeat(70));
}

// Función para testear limpieza de columnas (útil para debugging)
function testLimpiezaColumnas() {
    console.log('\n🧪 TEST LIMPIEZA DE COLUMNAS:');
    
    const ejemplosHeaders = [
        // Ejemplo 1: headers que podrían venir de hoja "5", "10", "15" (con metadatos duplicados)
        ['Id Jugador', 'Distancia Total', 'Velocidad Máxima', 'temporada', 'competicion', 'jornada', 'equipo', 'Sprints'],
        
        // Ejemplo 2: headers que podrían venir de hoja "Físico" (sin metadatos duplicados) 
        ['Id Jugador', 'Carga de Trabajo', 'Tiempo Jugado', 'Impactos'],
        
        // Ejemplo 3: headers mixtos
        ['Id Jugador', 'Distancia', 'liga', 'Velocidad', 'partido', 'Aceleraciones']
    ];
    
    ejemplosHeaders.forEach((headers, index) => {
        console.log(`\nEjemplo ${index + 1}:`);
        console.log(`  📥 Headers originales: ${headers.join(', ')}`);
        
        const headersDesdeColumna2 = headers.slice(1);
        const headersLimpios = limpiarColumnasMetadatos(headersDesdeColumna2);
        
        console.log(`  🧹 Headers limpios: ${headersLimpios.join(', ')}`);
        console.log(`  📊 Removidos: ${headersDesdeColumna2.length - headersLimpios.length} columnas`);
        
        // Simular añadir metadatos
        const columnasFinales = [...headersLimpios, 'Temporada', 'Competicion', 'Jornada', 'Partido', 'Equipo', 'tipo_reporte', 'hoja', 'archivo_origen'];
        console.log(`  ✅ Resultado final: ${columnasFinales.join(', ')}`);
    });
}

// Función para testear extracción de equipos
function testExtraccionEquipo() {
    const ejemplos = [
        "Informe de Rendimiento Físico Sevilla FC",
        "Informe de Rendimiento Físico Intervalos 5' Sevilla FC", 
        "Informe de Rendimiento Físico Intervalos 10' Sevilla FC",
        "Informe de Rendimiento Físico Intervalos 15' Sevilla FC",
        "Informe de Rendimiento Físico Girona FC",
        "Informe de Rendimiento Físico Real Madrid",
        "Informe de Rendimiento Físico Intervalos 5' FC Barcelona"
    ];

    console.log("🧪 Testing patrones de extracción de equipos:");
    
    ejemplos.forEach(ejemplo => {
        const resultado = testPatron(ejemplo);
        console.log(`  "${ejemplo}" → "${resultado}"`);
    });
}

function testPatron(texto) {
    const patrones = [
        /informe\s+de\s+rendimiento\s+físico\s+intervalos\s+\d+['´]\s+(.+)/i,
        /informe\s+de\s+rendimiento\s+físico\s+(?!intervalos)(.+)/i,
        /informe\s+de\s+rendimiento\s+intervalos\s+\d+['´]\s+(.+)/i,
        /informe\s+de\s+rendimiento\s+(?!físico|intervalos)(.+)/i,
        /informe\s+de\s+rendimiento\s+(.+)/i
    ];

    for (const patron of patrones) {
        const match = texto.match(patron);
        if (match && match[1]) {
            let equipo = match[1].trim();
            
            // Aplicar misma limpieza que en la función principal
            equipo = equipo.replace(/\s*(vs\.?|contra|v\.?)\s+.*/i, '');
            equipo = equipo.replace(/\s*-\s*.*/i, '');
            equipo = equipo.replace(/\s*\(.*\)/g, '');
            equipo = equipo.replace(/\s*\[.*\]/g, '');
            equipo = equipo.replace(/\s+(jornada|partido|fecha|temporada)\s+.*/i, '');
            
            if (equipo.length > 2 && equipo.length < 50 && !equipo.match(/^\d+$/)) {
                return equipo;
            }
        }
    }
    return "NO ENCONTRADO";
}

async function main() {
    console.log('🚀 Iniciando procesamiento incremental de archivos XLSX multi-hoja...');
    console.log(`🔍 Buscando en: ${BASE_PATH}`);
    console.log('='.repeat(70));
    
    // Crear carpeta data si no existe
    if (!fs.existsSync(OUTPUT_BASE_PATH)) {
        fs.mkdirSync(OUTPUT_BASE_PATH);
        console.log('📁 Carpeta data creada');
    }
    
    // Verificar que existe la ruta base
    if (!fs.existsSync(BASE_PATH)) {
        console.log(`❌ No se encuentra la ruta: ${BASE_PATH}`);
        return;
    }
    
    console.log('-'.repeat(70));
    
    // Buscar carpetas de partidos
    const carpetas = fs.readdirSync(BASE_PATH).filter(item => {
        const carpetaPath = path.join(BASE_PATH, item);
        return fs.statSync(carpetaPath).isDirectory();
    });
    
    console.log(`📁 Carpetas de partidos encontradas: ${carpetas.length}`);
    
    // Estructura para almacenar datos por hoja
    const datosPorHoja = {};
    let carpetasProcesadas = 0;
    let archivosExitosos = 0;
    let errores = 0;
    
    for (const carpeta of carpetas) {
        const carpetaPath = path.join(BASE_PATH, carpeta);
        const { jornada, partido } = extraerJornadaPartido(carpeta);
        
        console.log(`\n📂 Procesando: ${carpeta}`);
        console.log(`  📊 Jornada: ${jornada}, Partido: ${partido}`);
        
        const archivos = buscarArchivosXlsxEnCarpeta(carpetaPath);
        
        if (!archivos.rendimiento1 && !archivos.rendimiento2) {
            console.log(`  ❌ No se encontraron archivos rendimiento_1 o rendimiento_2`);
            errores++;
            continue;
        }
        
        carpetasProcesadas++;
        let archivosEnEstaCarpeta = 0; // Contador para esta carpeta específica
        
        // Procesar rendimiento_1
        if (archivos.rendimiento1) {
            console.log(`  🔄 Procesando rendimiento_1...`);
            const resultadosHojas1 = procesarArchivoXlsx(archivos.rendimiento1, jornada, partido, 'rendimiento_1');
            if (resultadosHojas1) {
                Object.entries(resultadosHojas1).forEach(([nombreHoja, datos]) => {
                    if (!datosPorHoja[nombreHoja]) {
                        datosPorHoja[nombreHoja] = [];
                    }
                    datosPorHoja[nombreHoja].push(...datos);
                });
                archivosExitosos++;
                archivosEnEstaCarpeta++;
                console.log(`    ✅ rendimiento_1 procesado exitosamente`);
            } else {
                errores++;
                console.log(`    ❌ Error procesando rendimiento_1`);
            }
        } else {
            console.log(`  ⚠️ rendimiento_1 NO encontrado`);
        }
        
        // Procesar rendimiento_2
        if (archivos.rendimiento2) {
            console.log(`  🔄 Procesando rendimiento_2...`);
            const resultadosHojas2 = procesarArchivoXlsx(archivos.rendimiento2, jornada, partido, 'rendimiento_2');
            if (resultadosHojas2) {
                Object.entries(resultadosHojas2).forEach(([nombreHoja, datos]) => {
                    if (!datosPorHoja[nombreHoja]) {
                        datosPorHoja[nombreHoja] = [];
                    }
                    datosPorHoja[nombreHoja].push(...datos);
                });
                archivosExitosos++;
                archivosEnEstaCarpeta++;
                console.log(`    ✅ rendimiento_2 procesado exitosamente`);
            } else {
                errores++;
                console.log(`    ❌ Error procesando rendimiento_2`);
            }
        } else {
            console.log(`  ⚠️ rendimiento_2 NO encontrado`);
        }
        
        console.log(`  📊 Archivos procesados en esta carpeta: ${archivosEnEstaCarpeta}/2`);
        console.log('-'.repeat(50));
    }
    
    console.log(`\n📊 Resumen del procesamiento:`);
    console.log(`  📁 Carpetas procesadas: ${carpetasProcesadas}`);
    console.log(`  ✅ Archivos procesados exitosamente: ${archivosExitosos}`);
    console.log(`  ❌ Archivos con errores: ${errores}`);
    console.log(`  📄 Hojas encontradas: ${Object.keys(datosPorHoja).join(', ')}`);
    
    if (Object.keys(datosPorHoja).length === 0) {
        console.log('\n⚠️ No se obtuvieron datos de ninguna hoja. No se crean parquets.');
        return;
    }
    
    // Procesar cada hoja por separado
    const estadisticasFinales = {
        parquetsCreados: 0,
        totalFilasNuevas: 0,
        hojasProcesadas: Object.keys(datosPorHoja).length
    };
    
    for (const [nombreHoja, datosNuevosSinFiltrar] of Object.entries(datosPorHoja)) {
        console.log(`\n🔄 Procesando hoja: ${nombreHoja}`);
        console.log(`  📥 Filas candidatas: ${datosNuevosSinFiltrar.length}`);
        
        if (datosNuevosSinFiltrar.length === 0) {
            console.log(`  ⚠️ No hay datos para la hoja ${nombreHoja}`);
            continue;
        }
        
        // Determinar nombre del archivo parquet
        const nombreArchivoParquet = obtenerNombreArchivoParquet(nombreHoja);
        const rutaCompleta = path.join(OUTPUT_BASE_PATH, nombreArchivoParquet);
        
        console.log(`  💾 Archivo destino: ${nombreArchivoParquet}`);
        
        // Cargar datos existentes de este parquet específico
        const datosExistentes = await leerDatosParquetExistentes(rutaCompleta);
        const clavesExistentes = crearConjuntoDuplicados(datosExistentes);
        
        // Filtrar duplicados
        console.log(`  🔍 Filtrando duplicados...`);
        const resultadoFiltrado = filtrarDatosDuplicados(datosNuevosSinFiltrar, clavesExistentes);
        const datosNuevosUnicos = resultadoFiltrado.datosFiltrados;
        
        console.log(`  📈 Resultados del filtrado:`);
        console.log(`    🆕 Filas nuevas únicas: ${datosNuevosUnicos.length}`);
        console.log(`    🚫 Duplicados evitados: ${resultadoFiltrado.totalDuplicados + resultadoFiltrado.duplicadosInternos}`);
        
        if (datosNuevosUnicos.length === 0) {
            console.log(`  ⚠️ No hay datos nuevos únicos para la hoja ${nombreHoja}`);
            continue;
        }
        
        // Combinar datos existentes + nuevos únicos
        const todosCombinados = [...datosExistentes, ...datosNuevosUnicos];
        
        console.log(`  💾 Guardando ${todosCombinados.length} filas totales en ${nombreArchivoParquet}`);
        
        const exitoEscritura = await escribirParquet(todosCombinados, rutaCompleta);
        
        if (exitoEscritura) {
            console.log(`  ✅ Parquet ${nombreArchivoParquet} actualizado exitosamente!`);
            estadisticasFinales.parquetsCreados++;
            estadisticasFinales.totalFilasNuevas += datosNuevosUnicos.length;
        } else {
            console.log(`  ❌ Error al escribir ${nombreArchivoParquet}`);
        }
    }
    
    // 🔍 VERIFICAR CONSISTENCIA DE COLUMNAS
    verificarConsistenciaColumnas(datosPorHoja);
    
    // Resumen final
    console.log(`\n🎉 Proceso completado!`);
    console.log(`📊 Estadísticas finales:`);
    console.log(`  📄 Hojas procesadas: ${estadisticasFinales.hojasProcesadas}`);
    console.log(`  💾 Parquets creados/actualizados: ${estadisticasFinales.parquetsCreados}`);
    console.log(`  📁 Carpetas procesadas: ${carpetasProcesadas}`);
    console.log(`  📄 Archivos xlsx procesados: ${archivosExitosos}`);
    console.log(`  🆕 Total filas nuevas añadidas: ${estadisticasFinales.totalFilasNuevas}`);
    console.log(`  📂 Carpeta de salida: ${OUTPUT_BASE_PATH}`);
    
    // Mostrar archivos parquet creados
    console.log(`\n📂 Archivos parquet generados:`);
    Object.keys(datosPorHoja).forEach(nombreHoja => {
        const nombreArchivo = obtenerNombreArchivoParquet(nombreHoja);
        const rutaCompleta = path.join(OUTPUT_BASE_PATH, nombreArchivo);
        if (fs.existsSync(rutaCompleta)) {
            console.log(`  💾 ${nombreArchivo} (hoja: ${nombreHoja})`);
        }
    });
}

// Verificar si es el archivo principal
if (require.main === module) {
    // Descomenta para ejecutar diagnósticos/tests antes del proceso principal:
    // diagnosticarNombresArchivos(BASE_PATH);
    // testLimpiezaColumnas();
    // testExtraccionEquipo();
    
    main().catch(console.error);
}

module.exports = { 
    procesarArchivoXlsx,
    procesarHojaXlsx,
    extraerJornadaPartido,
    buscarArchivosXlsxEnCarpeta,
    escribirParquet, 
    leerDatosParquetExistentes, 
    generarClaveUnica, 
    filtrarDatosDuplicados,
    obtenerNombreArchivoParquet,
    extraerEquipo,
    limpiarColumnasMetadatos,
    verificarConsistenciaColumnas
};