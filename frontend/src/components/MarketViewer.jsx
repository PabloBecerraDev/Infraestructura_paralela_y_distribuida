import React, { useState } from 'react'
import * as d3 from 'd3'

function MarketViewer() {
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(false)

  const fetchSPY = async () => {
    setLoading(true)
    const response = await fetch('https://query1.finance.yahoo.com/v7/finance/download/SPY?period1=1672444800&period2=1703980800&interval=1d&events=history')
    const csvText = await response.text()
    const parsed = d3.csvParse(csvText)
    setData(parsed)
    setLoading(false)
  }

  return (
    <div className="card">
      <h2>📈 Datos de Mercado (SPY)</h2>
      <button onClick={fetchSPY}>Descargar SPY</button>
      {loading && <p>Cargando SPY...</p>}
      {data.length > 0 && (
        <p>Días descargados: {data.length}</p>
      )}
    </div>
  )
}

export default MarketViewer
