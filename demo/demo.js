import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.js'

const app = document.getElementById('app')
if (!app) throw new Error('missing app element')

const params = new URLSearchParams(location.search)
const url = params.get('key') || undefined

const root = ReactDOM.createRoot(app)
root.render(React.createElement(App, { url }))
