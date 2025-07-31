import React, { useState } from 'react'

function ClusterViewer() {
  const [tickers, setTickers] = useState([])
  const [loading, setLoading] = useState(false)

  const handleClick = async () => {
    setLoading(true)
    const response = await fetch('http://localhost:5002/Kmeans-getData', {
      method: 'POST'
    })
    const result = await response.json()
    const uniqueTickers = [...new Set(result.map(row => row.ticker))]
    setTickers(uniqueTickers)
    setLoading(false)
  }

  return (
    <div className="card">
      <h2>🔀 Clustering Financiero (K-Means)</h2>
      <button onClick={handleClick}>Agrupar Activos</button>
      {loading && <p>Ejecutando clustering...</p>}
      {tickers.length > 0 && (
        <p>Activos agrupados: {tickers.length}</p>
      )}
    </div>
  )
}

export default ClusterViewer
