const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');
const parquet = require('parquetjs');

// Nombre fijo para el archivo parquet
const NOMBRE_PARQUET = 'datos_combinados.parquet';

async function leerDatosParquetExistentes() {
    try {
        if (fs.existsSync(NOMBRE_PARQUET)) {
            console.log('📖 Cargando datos existentes del parquet...');
            const reader = await parquet.ParquetReader.openFile(NOMBRE_PARQUET);
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

function extraerInfoPartido(texto) {
    try {
        const partes = texto.split('|').map(parte => parte.trim());
        
        if (partes.length >= 4) {
            const liga = partes[0];
            const temporada = partes[1].replace('Temporada ', '');
            const jornada = partes[2];
            const partidoYFecha = partes[3];
            
            const matchFecha = partidoYFecha.match(/\((\d{4}-\d{2}-\d{2})\)$/);
            let fecha = null;
            let partido = partidoYFecha;
            
            if (matchFecha) {
                fecha = matchFecha[1];
                partido = partidoYFecha.substring(0, matchFecha.index).trim();
            }
            
            return { liga, temporada, jornada, partido, fecha };
        }
        return null;
    } catch (error) {
        console.log(`Error extrayendo info del partido: ${error}`);
        return null;
    }
}

function extraerEquipo(sheet) {
    const range = XLSX.utils.decode_range(sheet['!ref']);

    for (let row = 0; row <= Math.min(30, range.e.r); row++) {
        for (let col = 0; col <= Math.min(20, range.e.c); col++) {
            const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
            const cell = sheet[cellAddress];

            if (cell && typeof cell.v === 'string') {
                const valor = cell.v.trim().toLowerCase();

                if (valor.includes('informe de rendimiento físico')) {
                    const match = cell.v.match(/Informe de Rendimiento Físico\s+(.*)/i);
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

function encontrarInfoPartido(sheet) {
    const range = XLSX.utils.decode_range(sheet['!ref']);
    
    for (let row = 0; row <= Math.min(15, range.e.r); row++) {
        for (let col = 0; col <= Math.min(15, range.e.c); col++) {
            const cellAddress = XLSX.utils.encode_cell({r: row, c: col});
            const cell = sheet[cellAddress];
            
            if (cell && cell.v && typeof cell.v === 'string' && 
                cell.v.includes('|') && cell.v.includes('Temporada')) {
                return extraerInfoPartido(cell.v);
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

function procesarArchivoXlsx(archivoPath) {
    const nombreArchivo = path.basename(archivoPath);
    console.log(`📄 Procesando: ${nombreArchivo}`);
    
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
        
        const infoPartido = encontrarInfoPartido(sheet);
        if (!infoPartido) {
            console.log(`  ❌ No se encontró información del partido`);
            return null;
        }
        
        const equipo = extraerEquipo(sheet);
        if (!equipo) {
            console.log(`  ❌ No se encontró el equipo (Informe de Rendimiento Físico)`);
            return null;
        }

        const filaHeaders = encontrarFilaHeaders(sheet);
        if (filaHeaders === null) {
            console.log(`  ❌ No se encontró fila con 'Id Jugador'`);
            return null;
        }
        
        console.log(`  📍 Headers en fila: ${filaHeaders + 1}`);
        console.log(`  ⚽ ${infoPartido.jornada} | ${infoPartido.partido}`);
        console.log(`  🏃 Equipo: ${equipo}`);
        
        const jsonData = XLSX.utils.sheet_to_json(sheet, {
            range: filaHeaders,
            header: 1
        });
        
        if (jsonData.length === 0) {
            console.log(`  ❌ No hay datos`);
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
            obj.liga = infoPartido.liga;
            obj.temporada = infoPartido.temporada;
            obj.jornada = infoPartido.jornada;
            obj.partido = infoPartido.partido;
            obj.fecha = infoPartido.fecha;
            obj.archivo_origen = nombreArchivo;
            
            return obj;
        }).filter(row => {
            return Object.values(row).some(val => val !== null && val !== undefined && val !== '');
        });
        
        console.log(`  ✅ Procesadas ${datosProcessed.length} filas`);
        return datosProcessed;
        
    } catch (error) {
        console.log(`  ❌ Error: ${error.message}`);
        return null;
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

function borrarArchivo(archivoPath) {
    try {
        fs.unlinkSync(archivoPath);
        console.log(`  🗑️ Archivo borrado: ${path.basename(archivoPath)}`);
        return true;
    } catch (error) {
        console.log(`  ⚠️ Error borrando archivo: ${error.message}`);
        return false;
    }
}

async function main() {
    console.log('🚀 Iniciando procesamiento incremental de Informes de Rendimiento Físico...');
    console.log('='.repeat(65));
    
    // Buscar archivos xlsx en directorio actual
    const archivosXlsx = fs.readdirSync('.')
        .filter(file => file.endsWith('.xlsx'))
        .map(file => path.resolve(file));
    
    console.log(`📁 Archivos .xlsx encontrados: ${archivosXlsx.length}`);
    
    if (archivosXlsx.length === 0) {
        console.log('✅ No hay archivos xlsx nuevos que procesar');
        return;
    }
    
    // Cargar datos existentes del parquet
    const datosExistentes = await leerDatosParquetExistentes();
    
    console.log('-'.repeat(65));
    
    // Procesar archivos xlsx
    const datosNuevos = [];
    const archivosExitosos = [];
    let procesados = 0;
    let errores = 0;
    
    for (const archivo of archivosXlsx) {
        const datos = procesarArchivoXlsx(archivo);
        if (datos && datos.length > 0) {
            datosNuevos.push(...datos);
            archivosExitosos.push(archivo);
            procesados++;
        } else {
            errores++;
        }
        console.log('-'.repeat(45));
    }
    
    console.log(`\n📊 Resumen del procesamiento:`);
    console.log(`  ✅ Archivos procesados exitosamente: ${procesados}`);
    console.log(`  ❌ Archivos con errores: ${errores}`);
    console.log(`  📈 Nuevas filas obtenidas: ${datosNuevos.length}`);
    console.log(`  📚 Filas existentes en parquet: ${datosExistentes.length}`);
    
    if (datosNuevos.length === 0) {
        console.log('\n⚠️ No se obtuvieron datos nuevos. No se actualiza el parquet.');
        return;
    }
    
    // Combinar datos existentes + nuevos
    const todosCombinados = [...datosExistentes, ...datosNuevos];
    
    console.log(`\n💾 Guardando ${todosCombinados.length} filas totales en: ${NOMBRE_PARQUET}`);
    
    const exitoEscritura = await escribirParquet(todosCombinados, NOMBRE_PARQUET);
    
    if (exitoEscritura) {
        console.log(`✅ Parquet actualizado exitosamente!`);
        
        // Borrar archivos xlsx procesados exitosamente
        console.log(`\n🗑️ Borrando archivos xlsx procesados...`);
        let archivosBorrados = 0;
        
        archivosExitosos.forEach(archivo => {
            if (borrarArchivo(archivo)) {
                archivosBorrados++;
            }
        });
        
        console.log(`\n🎉 Proceso completado exitosamente!`);
        console.log(`📊 Estadísticas finales:`);
        console.log(`  📈 Total filas en parquet: ${todosCombinados.length}`);
        console.log(`  🆕 Filas nuevas agregadas: ${datosNuevos.length}`);
        console.log(`  📄 Archivos xlsx procesados: ${procesados}`);
        console.log(`  🗑️ Archivos xlsx borrados: ${archivosBorrados}`);
        console.log(`  💾 Archivo parquet: ${NOMBRE_PARQUET}`);
        
        // Mostrar resumen por partido de los datos nuevos
        if (datosNuevos.length > 0) {
            console.log(`\n📈 Resumen de datos nuevos agregados:`);
            const resumenNuevos = {};
            datosNuevos.forEach(row => {
                const key = `${row.jornada} | ${row.partido} | ${row.equipo}`;
                resumenNuevos[key] = (resumenNuevos[key] || 0) + 1;
            });
            
            Object.entries(resumenNuevos).forEach(([partidoEquipo, count]) => {
                console.log(`  🏃 ${partidoEquipo}: ${count} filas`);
            });
        }
        
    } else {
        console.log(`\n❌ Error al escribir el parquet. Los archivos xlsx NO han sido borrados.`);
        
        // Fallback a JSON
        const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
        const nombreJson = `datos_fallback_${timestamp}.json`;
        fs.writeFileSync(nombreJson, JSON.stringify(todosCombinados, null, 2));
        console.log(`💾 Datos guardados como fallback en: ${nombreJson}`);
    }
}

// Verificar si es el archivo principal
if (require.main === module) {
    main().catch(console.error);
}

module.exports = { procesarArchivoXlsx, extraerInfoPartido, escribirParquet, leerDatosParquetExistentes };