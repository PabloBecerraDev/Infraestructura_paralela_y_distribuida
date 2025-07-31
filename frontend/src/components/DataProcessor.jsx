import React, { useState } from 'react';

function DataProcessor() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [total, setTotal] = useState(0);

  const handleClick = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://localhost:5050/api/process', {
        method: 'POST',
      });
      const result = await response.json();
      setTotal(result.length);
      setData(result.slice(0, 5)); // Mostrar solo los primeros 5
    } catch (error) {
      console.error('Error en el procesamiento de datos:', error);
    }
    setLoading(false);
  };

  return (
    <div className="card">
      <h2>🛠 Procesamiento de Datos Financieros</h2>
      <button onClick={handleClick}>Procesar Datos (Paralelo)</button>
      {loading && <p>Procesando...</p>}

      {!loading && total > 0 && (
        <>
          <p>Datos procesados: {total}</p>
          <table border="1" cellPadding="5">
            <thead>
              <tr>
                <th>#</th>
                <th>Ticker</th>
                <th>RSI</th>
                <th>MACD</th>
                <th>ATR</th>
              </tr>
            </thead>
            <tbody>
              {data.map((row, index) => (
                <tr key={index}>
                  <td>{index + 1}</td>
                  <td>{row.ticker || '-'}</td>
                  <td>{row.rsi?.toFixed(2) ?? '-'}</td>
                  <td>{row.macd?.toFixed(2) ?? '-'}</td>
                  <td>{row.atr?.toFixed(2) ?? '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}

export default DataProcessor;
