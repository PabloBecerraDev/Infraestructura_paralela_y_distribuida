import React, { useState } from 'react'

function SentimentViewer() {
  const [sentiment, setSentiment] = useState(null)
  const [loading, setLoading] = useState(false)

  const fetchSentiment = async () => {
    setLoading(true)
    const response = await fetch('http://localhost:5000/sentiment', {
      method: 'GET'
    })
    const result = await response.json()
    setSentiment(result)
    setLoading(false)
  }

  return (
    <div className="card">
      <h2>💬 Análisis de Sentimiento</h2>
      <button onClick={fetchSentiment}>Cargar Sentimiento</button>
      {loading && <p>Cargando...</p>}
      {sentiment && (
        <pre>{JSON.stringify(sentiment, null, 2)}</pre>
      )}
    </div>
  )
}

export default SentimentViewer
