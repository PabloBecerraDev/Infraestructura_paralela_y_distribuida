import React, { useState } from 'react';

function ClusterViewer() {
  const [tickers, setTickers] = useState([]);
  const [loading, setLoading] = useState(false);
  const [total, setTotal] = useState(0);

  const handleClick = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://localhost:5050/api/cluster', {
        method: 'POST',
      });
      const result = await response.json();

      const uniqueTickers = [...new Set(result.map(row => row.ticker))];
      setTotal(uniqueTickers.length);
      setTickers(uniqueTickers.slice(0, 5)); // Mostrar solo los primeros 5
    } catch (error) {
      console.error('Error en clustering:', error);
    }
    setLoading(false);
  };

  return (
    <div className="card">
      <h2>🔀 Clustering Financiero (K-Means)</h2>
      <button onClick={handleClick}>Agrupar Activos</button>
      {loading && <p>Ejecutando clustering...</p>}
      {!loading && total > 0 && (
        <>
          <p>Activos agrupados: {total}</p>
          <table border="1" cellPadding="5">
            <thead>
              <tr>
                <th>#</th>
                <th>Ticker</th>
              </tr>
            </thead>
            <tbody>
              {tickers.map((ticker, index) => (
                <tr key={index}>
                  <td>{index + 1}</td>
                  <td>{ticker}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}

export default ClusterViewer;
