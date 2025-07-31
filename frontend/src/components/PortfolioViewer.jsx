import React, { useState } from 'react'

function PortfolioViewer() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const fetchPortfolio = async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await fetch('http://localhost:5050/api/portfolio', {
        method: 'POST'
      })
      const result = await response.json()
      setData(result)
    } catch (err) {
      setError('No se pudo cargar el portafolio')
      console.error(err)
    }
    setLoading(false)
  }

  return (
    <div className="card">
      <h2>📊 Optimización de Portafolio</h2>
      <button onClick={fetchPortfolio}>Cargar Portafolio</button>
      {loading && <p>Cargando...</p>}
      {error && <p style={{ color: 'red' }}>{error}</p>}

      {data?.tickers?.length > 0 && (
        <div>
          <p>Activos recomendados: {data.tickers.length}</p>
          <table border="1" cellPadding="5">
            <thead>
              <tr>
                <th>Ticker</th>
                <th>Peso (%)</th>
              </tr>
            </thead>
            <tbody>
              {data.tickers.slice(0, 5).map((item, index) => (
                <tr key={index}>
                  <td>{item.ticker}</td>
                  <td>{(item.weight * 100).toFixed(2)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {data?.chart_url && (
        <div>
          <p>📈 Rendimiento del portafolio:</p>
          <img src={data.chart_url} alt="Retorno del portafolio" width="100%" />
        </div>
      )}
    </div>
  )
}

export default PortfolioViewer
