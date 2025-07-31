import React, { useState } from 'react';
import * as d3 from 'd3';

function MarketViewer() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [total, setTotal] = useState(0);

  const fetchSPY = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://localhost:5050/api/market'); // Gateway endpoint
      const csvText = await response.text();
      const parsed = d3.csvParse(csvText);
      setTotal(parsed.length);
      setData(parsed.slice(0, 5)); // Mostrar solo los primeros 5 días
    } catch (error) {
      console.error('Error al obtener datos del mercado:', error);
    }
    setLoading(false);
  };

  return (
    <div className="card">
      <h2>📈 Datos de Mercado (SPY)</h2>
      <button onClick={fetchSPY}>Descargar SPY</button>
      {loading && <p>Cargando SPY...</p>}

      {!loading && total > 0 && (
        <>
          <p>Días descargados: {total}</p>
          <table border="1" cellPadding="5">
            <thead>
              <tr>
                <th>Fecha</th>
                <th>Apertura</th>
                <th>Máximo</th>
                <th>Mínimo</th>
                <th>Cierre</th>
                <th>Volumen</th>
              </tr>
            </thead>
            <tbody>
              {data.map((row, i) => (
                <tr key={i}>
                  <td>{row.Date}</td>
                  <td>{parseFloat(row.Open).toFixed(2)}</td>
                  <td>{parseFloat(row.High).toFixed(2)}</td>
                  <td>{parseFloat(row.Low).toFixed(2)}</td>
                  <td>{parseFloat(row.Close).toFixed(2)}</td>
                  <td>{row.Volume}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}

export default MarketViewer;
