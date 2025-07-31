import React from 'react'
import DataProcessor from './components/DataProcessor'
import ClusterViewer from './components/ClusterViewer'
import PortfolioViewer from './components/PortfolioViewer'
import MarketViewer from './components/MarketViewer'
import SentimentViewer from './components/SentimentViewer'

function App() {
  return (
    <div className="container">
      <h1>📊 Dashboard Financiero Distribuido</h1>
      <DataProcessor />
      <ClusterViewer />
      <PortfolioViewer />
      <MarketViewer />
      <SentimentViewer />
    </div>
  )
}

export default App
