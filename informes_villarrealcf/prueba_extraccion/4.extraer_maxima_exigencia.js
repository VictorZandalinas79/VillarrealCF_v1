const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');
const parquet = require('parquetjs');

// Configuración
const BASE_PATH = 'VCF_Mediacoach_Data/Temporada_24_25/La_Liga/Partidos';
const OUTPUT_PATH = 'data/maxima_exigencia.parquet';

async function leerDatosParquetExistentes() {
    try {
        if (fs.existsSync(OUTPUT_PATH)) {
            console.log('📖 Cargando datos existentes del parquet...');
            const reader = await parquet.ParquetReader.openFile(OUTPUT_PATH);
            const cursor = reader.getCursor();
            const datos = [];
            
            let record = null;
            while (record = await cursor.next()) {
                datos.push(record);
            }
            
            await reader.close();
            console.log(`✅ Cargadas ${datos.length} filas existentes`);
            return datos;
        } else {
            console.log('🆕 No existe archivo parquet previo, creando nuevo...');
            return [];
        }
    } catch (error) {
        console.log(`⚠️ Error leyendo parquet existente: ${error.message}`);
        console.log('🆕 Iniciando con datos vacíos...');
        return [];
    }
}

function generarClaveUnica(fila) {
    // Genera una clave única para detectar duplicados
    const campos = [
        fila['Id Jugador'] || fila['id_jugador'] || '',
        fila.liga || '',
        fila.temporada || '',
        fila.jornada || '',
        fila.partido || '',
        fila.equipo || '',
        fila.archivo_origen || '',
        fila.tipo_reporte || '' // Diferencia entre rendimiento_1 y rendimiento_2
    ];
    
    return campos.join('|').toLowerCase().trim();
}

function crearConjuntoDuplicados(datosExistentes) {
    const clavesExistentes = new Set();
    
    datosExistentes.forEach(fila => {
        const clave = generarClaveUnica(fila);
        if (clave && clave !== '|||||||') {
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
        
        if (!clave || clave === '|||||||') {
            console.log(`⚠️ Fila ${index + 1} tiene datos incompletos, se omite`);
            return;
        }
        
        if (clavesExistentes.has(clave)) {
            duplicadosEncontrados.push({
                fila: index + 1,
                jugador: fila['Id Jugador'] || 'Sin ID',
                partido: `${fila.jornada} | ${fila.partido}`,
                equipo: fila.equipo,
                tipo: fila.tipo_reporte
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

    for (let row = 0; row <= Math.min(30, range.e.r); row++) {
        for (let col = 0; col <= Math.min(20, range.e.c); col++) {
            const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
            const cell = sheet[cellAddress];

            if (cell && typeof cell.v === 'string') {
                const valor = cell.v.trim().toLowerCase();

                if (valor.includes('escenarios de máxima')) {
                    const match = cell.v.match(/Escenarios de Máxima\s+(.*)/i);
                    if (match && match[1]) {
                        const equipo = match[1].trim();
                        if (equipo.length > 0) {
                            return equipo;
                        }
                    }
                }
            }
        }
    }
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
        
        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];
        
        const equipo = extraerEquipo(sheet);
        if (!equipo) {
            console.log(`    ❌ No se encontró el equipo`);
            return null;
        }

        const filaHeaders = encontrarFilaHeaders(sheet);
        if (filaHeaders === null) {
            console.log(`    ❌ No se encontró fila con 'Id Jugador'`);
            return null;
        }
        
        console.log(`    📍 Headers en fila: ${filaHeaders + 1}`);
        console.log(`    🏃 Equipo: ${equipo}`);
        
        const jsonData = XLSX.utils.sheet_to_json(sheet, {
            range: filaHeaders,
            header: 1
        });
        
        if (jsonData.length === 0) {
            console.log(`    ❌ No hay datos`);
            return null;
        }
        
        const headers = jsonData[0];
        const headersFiltered = headers.slice(1); // Desde columna 2
        
        const datosProcessed = jsonData.slice(1).map(row => {
            const rowFiltered = row.slice(1); // Desde columna 2
            const obj = {};
            
            headersFiltered.forEach((header, index) => {
                obj[header || `columna_${index + 2}`] = rowFiltered[index];
            });
            
            obj.equipo = equipo;
            obj.liga = 'La Liga';
            obj.temporada = '24_25';
            obj.jornada = jornada;
            obj.partido = partido;
            obj.tipo_reporte = tipoReporte;
            obj.archivo_origen = nombreArchivo;
            
            return obj;
        }).filter(row => {
            return Object.values(row).some(val => val !== null && val !== undefined && val !== '');
        });
        
        console.log(`    ✅ Procesadas ${datosProcessed.length} filas`);
        return datosProcessed;
        
    } catch (error) {
        console.log(`    ❌ Error: ${error.message}`);
        return null;
    }
}

function buscarArchivosXlsxEnCarpeta(carpetaPath) {
    try {
        const archivos = fs.readdirSync(carpetaPath);
        
        // Primero buscar archivos maxima_exigencia
        let maxima1 = archivos.find(archivo => 
            archivo.toLowerCase().startsWith('maxima_exigencia_1') && archivo.endsWith('.xlsx')
        );
        
        let maxima2 = archivos.find(archivo => 
            archivo.toLowerCase().startsWith('maxima_exigencia_2') && archivo.endsWith('.xlsx')
        );
        
        let tipoArchivo = 'maxima_exigencia';
        
        // Si no se encuentran, buscar archivos otro_xlsx
        if (!maxima1 && !maxima2) {
            console.log(`    🔍 No se encontraron maxima_exigencia, buscando otro_xlsx...`);
            
            const archivosOtro = archivos.filter(archivo => 
                archivo.toLowerCase().startsWith('otro_xlsx') && archivo.endsWith('.xlsx')
            );
            
            if (archivosOtro.length >= 2) {
                // Tomar los dos primeros archivos otro_xlsx
                maxima1 = path.join(carpetaPath, archivosOtro[0]);
                maxima2 = path.join(carpetaPath, archivosOtro[1]);
                tipoArchivo = 'otro_xlsx';
                console.log(`    ✅ Encontrados archivos otro_xlsx: ${archivosOtro[0]}, ${archivosOtro[1]}`);
            } else if (archivosOtro.length === 1) {
                maxima1 = path.join(carpetaPath, archivosOtro[0]);
                tipoArchivo = 'otro_xlsx';
                console.log(`    ⚠️ Solo encontrado un archivo otro_xlsx: ${archivosOtro[0]}`);
            }
        } else {
            if (maxima1) maxima1 = path.join(carpetaPath, maxima1);
            if (maxima2) maxima2 = path.join(carpetaPath, maxima2);
        }
        
        return {
            maxima1,
            maxima2,
            tipoArchivo
        };
    } catch (error) {
        console.log(`    ❌ Error leyendo carpeta: ${error.message}`);
        return { maxima1: null, maxima2: null, tipoArchivo: 'desconocido' };
    }
}

async function escribirParquet(datos, nombreArchivo) {
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
        const writer = await parquet.ParquetWriter.openFile(schema, nombreArchivo);

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

async function main() {
    console.log('🚀 Iniciando procesamiento incremental de archivos XLSX de máxima exigencia...');
    console.log(`🔍 Buscando en: ${BASE_PATH}`);
    console.log('='.repeat(70));
    
    // Crear carpeta data si no existe
    if (!fs.existsSync('data')) {
        fs.mkdirSync('data');
        console.log('📁 Carpeta data creada');
    }
    
    // Verificar que existe la ruta base
    if (!fs.existsSync(BASE_PATH)) {
        console.log(`❌ No se encuentra la ruta: ${BASE_PATH}`);
        return;
    }
    
    // Cargar datos existentes del parquet
    const datosExistentes = await leerDatosParquetExistentes();
    const clavesExistentes = crearConjuntoDuplicados(datosExistentes);
    
    console.log('-'.repeat(70));
    
    // Buscar carpetas de partidos
    const carpetas = fs.readdirSync(BASE_PATH).filter(item => {
        const carpetaPath = path.join(BASE_PATH, item);
        return fs.statSync(carpetaPath).isDirectory();
    });
    
    console.log(`📁 Carpetas de partidos encontradas: ${carpetas.length}`);
    
    const datosNuevosSinFiltrar = [];
    let carpetasProcesadas = 0;
    let archivosExitosos = 0;
    let errores = 0;
    
    for (const carpeta of carpetas) {
        const carpetaPath = path.join(BASE_PATH, carpeta);
        const { jornada, partido } = extraerJornadaPartido(carpeta);
        
        console.log(`\n📂 Procesando: ${carpeta}`);
        console.log(`  📊 Jornada: ${jornada}, Partido: ${partido}`);
        
        const archivos = buscarArchivosXlsxEnCarpeta(carpetaPath);
        
        if (!archivos.maxima1 && !archivos.maxima2) {
            console.log(`  ❌ No se encontraron archivos de máxima exigencia ni otro_xlsx`);
            errores++;
            continue;
        }
        
        console.log(`  📋 Tipo de archivos encontrados: ${archivos.tipoArchivo}`);
        carpetasProcesadas++;
        
        // Determinar nombres de tipos de reporte según el tipo de archivo
        const tipoReporte1 = archivos.tipoArchivo === 'maxima_exigencia' ? 'maxima_exigencia_1' : 'otro_xlsx_1';
        const tipoReporte2 = archivos.tipoArchivo === 'maxima_exigencia' ? 'maxima_exigencia_2' : 'otro_xlsx_2';
        
        // Procesar primer archivo
        if (archivos.maxima1) {
            const datos1 = procesarArchivoXlsx(archivos.maxima1, jornada, partido, tipoReporte1);
            if (datos1 && datos1.length > 0) {
                datosNuevosSinFiltrar.push(...datos1);
                archivosExitosos++;
            } else {
                errores++;
            }
        }
        
        // Procesar segundo archivo
        if (archivos.maxima2) {
            const datos2 = procesarArchivoXlsx(archivos.maxima2, jornada, partido, tipoReporte2);
            if (datos2 && datos2.length > 0) {
                datosNuevosSinFiltrar.push(...datos2);
                archivosExitosos++;
            } else {
                errores++;
            }
        }
        
        console.log('-'.repeat(50));
    }
    
    console.log(`\n📊 Resumen del procesamiento:`);
    console.log(`  📁 Carpetas procesadas: ${carpetasProcesadas}`);
    console.log(`  ✅ Archivos procesados exitosamente: ${archivosExitosos}`);
    console.log(`  ❌ Archivos con errores: ${errores}`);
    console.log(`  📥 Filas candidatas obtenidas: ${datosNuevosSinFiltrar.length}`);
    console.log(`  📚 Filas existentes en parquet: ${datosExistentes.length}`);
    
    if (datosNuevosSinFiltrar.length === 0) {
        console.log('\n⚠️ No se obtuvieron datos nuevos. No se actualiza el parquet.');
        return;
    }
    
    // Filtrar duplicados
    console.log('\n🔍 Filtrando duplicados...');
    const resultadoFiltrado = filtrarDatosDuplicados(datosNuevosSinFiltrar, clavesExistentes);
    const datosNuevosUnicos = resultadoFiltrado.datosFiltrados;
    
    console.log(`\n📈 Resultados del filtrado:`);
    console.log(`  🆕 Filas nuevas únicas: ${datosNuevosUnicos.length}`);
    console.log(`  🚫 Duplicados con datos existentes: ${resultadoFiltrado.totalDuplicados}`);
    console.log(`  🔄 Duplicados internos: ${resultadoFiltrado.duplicadosInternos}`);
    
    if (datosNuevosUnicos.length === 0) {
        console.log('\n⚠️ No hay datos nuevos únicos para añadir. El parquet no se modifica.');
        return;
    }
    
    // Combinar datos existentes + nuevos únicos
    const todosCombinados = [...datosExistentes, ...datosNuevosUnicos];
    
    console.log(`\n💾 Guardando ${todosCombinados.length} filas totales en: ${OUTPUT_PATH}`);
    console.log(`  📚 Filas existentes preservadas: ${datosExistentes.length}`);
    console.log(`  🆕 Filas nuevas añadidas: ${datosNuevosUnicos.length}`);
    
    const exitoEscritura = await escribirParquet(todosCombinados, OUTPUT_PATH);
    
    if (exitoEscritura) {
        console.log(`✅ Parquet actualizado exitosamente!`);
        
        console.log(`\n🎉 Proceso completado exitosamente!`);
        console.log(`📊 Estadísticas finales:`);
        console.log(`  📈 Total filas en parquet: ${todosCombinados.length}`);
        console.log(`  📁 Carpetas procesadas: ${carpetasProcesadas}`);
        console.log(`  📄 Archivos xlsx procesados: ${archivosExitosos}`);
        console.log(`  🆕 Filas nuevas añadidas: ${datosNuevosUnicos.length}`);
        console.log(`  🚫 Duplicados evitados: ${resultadoFiltrado.totalDuplicados + resultadoFiltrado.duplicadosInternos}`);
        console.log(`  💾 Archivo parquet: ${OUTPUT_PATH}`);
        
        // Mostrar resumen por partido de los datos nuevos
        if (datosNuevosUnicos.length > 0) {
            console.log(`\n📈 Resumen de datos nuevos por partido:`);
            const resumenNuevos = {};
            datosNuevosUnicos.forEach(row => {
                const key = `${row.jornada} | ${row.partido} | ${row.tipo_reporte}`;
                resumenNuevos[key] = (resumenNuevos[key] || 0) + 1;
            });
            
            Object.entries(resumenNuevos).forEach(([partidoTipo, count]) => {
                console.log(`  ⚡ ${partidoTipo}: ${count} filas`);
            });
        }
        
    } else {
        console.log(`\n❌ Error al escribir el parquet.`);
    }
}

// Verificar si es el archivo principal
if (require.main === module) {
    main().catch(console.error);
}

module.exports = { 
    procesarArchivoXlsx, 
    extraerJornadaPartido,
    buscarArchivosXlsxEnCarpeta,
    escribirParquet, 
    leerDatosParquetExistentes, 
    generarClaveUnica, 
    filtrarDatosDuplicados 
};