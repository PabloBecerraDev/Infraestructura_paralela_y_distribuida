import React, { useState } from 'react'

function PortfolioViewer() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  const fetchPortfolio = async () => {
    setLoading(true)
    const response = await fetch('http://localhost:5003/getResult', {
      method: 'POST'
    })
    const result = await response.json()
    setData(result)
    setLoading(false)
  }

  return (
    <div className="card">
      <h2>📊 Optimización de Portafolio</h2>
      <button onClick={fetchPortfolio}>Cargar Portafolio</button>
      {loading && <p>Cargando...</p>}
      {data?.chart_url && (
        <img src={data.chart_url} alt="Retorno del portafolio" width="100%" />
      )}
    </div>
  )
}

export default PortfolioViewer
