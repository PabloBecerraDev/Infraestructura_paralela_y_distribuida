import React, { useState } from 'react'

function DataProcessor() {
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(false)

  const handleClick = async () => {
    setLoading(true)
    const response = await fetch('http://localhost:5001/getData', {
      method: 'POST'
    })
    const result = await response.json()
    setData(result)
    setLoading(false)
  }

  return (
    <div className="card">
      <h2>🛠 Procesamiento de Datos Financieros</h2>
      <button onClick={handleClick}>Procesar Datos (Paralelo)</button>
      {loading && <p>Procesando...</p>}
      {data.length > 0 && <p>Datos procesados: {data.length}</p>}
    </div>
  )
}

export default DataProcessor
