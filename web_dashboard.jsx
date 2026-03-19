import React, { useState, useEffect } from 'react';

const MarketRegimeHeatmap = () => {
  const instruments = ['EUR/USD', 'Oil', 'Gold', 'S&P 500', 'VIX', 'Bund Yield'];
  
  const [correlations, setCorrelations] = useState({
    'EUR/USD': [1.00, 0.00, 0.00, 0.00, 0.00, 0.00],
    'Oil': [0.00, 1.00, 0.00, 0.00, 0.00, 0.00],
    'Gold': [0.00, 0.00, 1.00, 0.00, 0.00, 0.00],
    'S&P 500': [0.00, 0.00, 0.00, 1.00, 0.00, 0.00],
    'VIX': [0.00, 0.00, 0.00, 0.00, 1.00, 0.00],
    'Bund Yield': [0.00, 0.00, 0.00, 0.00, 0.00, 1.00]
  });
  
  const [stats, setStats] = useState({ maxPos: 0, maxNeg: 0, regime: 'WAITING' });
  const [timestamp, setTimestamp] = useState(new Date().toLocaleTimeString());
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('http://localhost:5001/api/correlations');
        const data = await response.json();
        
        setConnected(true);
        setTimestamp(new Date().toLocaleTimeString());
        
        // Build correlation matrix from API data
        const updated = { ...correlations };
        const apiCorrs = data.correlations;
        
        // Map API field names to instrument names
        const nameMap = {
          'eur_usd': 'EUR/USD',
          'oil': 'Oil',
          'gold': 'Gold',
          'sp500': 'S&P 500',
          'vix': 'VIX',
          'bund_yield': 'Bund Yield'
        };
        
        // Fill in the correlation matrix
        instruments.forEach((instr, i) => {
          updated[instr] = [1.0, 0, 0, 0, 0, 0];
          
          // EUR/USD is always index 0
          if (instr === 'EUR/USD') {
            updated[instr][0] = 1.0;
            updated[instr][1] = apiCorrs.oil || 0;
            updated[instr][2] = apiCorrs.gold || 0;
            updated[instr][3] = apiCorrs.sp500 || 0;
            updated[instr][4] = apiCorrs.vix || 0;
            updated[instr][5] = apiCorrs.bund_yield || 0;
          } else {
            // Other instruments: correlation with EUR/USD
            const otherCorr = apiCorrs[Object.keys(nameMap).find(k => nameMap[k] === instr)] || 0;
            updated[instr][0] = otherCorr;
            updated[instr][instruments.indexOf(instr)] = 1.0;
          }
        });
        
        setCorrelations(updated);
        
        // Update stats
        const values = Object.values(apiCorrs).filter(v => typeof v === 'number');
        const maxPos = Math.max(...values.filter(v => v > 0), 0);
        const maxNeg = Math.min(...values.filter(v => v < 0), 0);
        setStats({ maxPos, maxNeg, regime: data.regime.toUpperCase() });
      } catch (error) {
        setConnected(false);
        console.error('Failed to fetch correlations:', error);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 2000);
    return () => clearInterval(interval);
  }, []);

  const getCellClass = (value) => {
    const abs = Math.abs(value);
    if (value >= 0.7) return 'cell-high-positive';
    if (value >= 0.3) return 'cell-positive';
    if (value > -0.3) return 'cell-neutral';
    if (value >= -0.7) return 'cell-negative';
    return 'cell-high-negative';
  };

  const formatCorr = (value) => {
    return (value >= 0 ? '+' : '') + value.toFixed(2);
  };

  const styles = {
    container: {
      padding: '2rem',
      background: '#0a0e27',
      fontFamily: "'Monaco', 'Courier New', monospace",
      minHeight: '100vh'
    },
    header: {
      marginBottom: '2rem'
    },
    title: {
      fontSize: '28px',
      fontWeight: 700,
      color: '#00ff88',
      textTransform: 'uppercase',
      letterSpacing: '2px',
      margin: 0
    },
    subtitle: {
      fontSize: '13px',
      color: '#888',
      margin: '4px 0 0 0'
    },
    timestamp: {
      fontSize: '11px',
      color: '#555',
      marginTop: '8px',
      fontStyle: 'italic'
    },
    heatmapWrapper: {
      background: '#111830',
      border: '1px solid #1a2547',
      borderRadius: '8px',
      padding: '1.5rem',
      overflowX: 'auto'
    },
    table: {
      borderCollapse: 'collapse',
      fontSize: '12px',
      minWidth: '100%'
    },
    th: {
      color: '#888',
      fontWeight: 500,
      padding: '12px 8px',
      textAlign: 'center',
      borderBottom: '1px solid #1a2547'
    },
    rowHeader: {
      color: '#888',
      fontWeight: 500,
      textAlign: 'right',
      padding: '8px 16px 8px 0',
      borderRight: '1px solid #1a2547',
      fontSize: '12px'
    },
    cell: {
      width: '48px',
      height: '48px',
      padding: 0,
      textAlign: 'center',
      verticalAlign: 'middle',
      cursor: 'pointer',
      position: 'relative',
      border: '0.5px solid #0a0e27',
      transition: 'transform 0.2s, box-shadow 0.2s',
      fontWeight: 600,
      fontSize: '11px'
    },
    cellPositive: {
      background: 'linear-gradient(135deg, #00ff88 0%, #00cc6f 100%)',
      color: '#0a0e27'
    },
    cellNeutral: {
      background: '#1a1f2e',
      color: '#666'
    },
    cellNegative: {
      background: '#3a3f4d',
      color: '#888'
    },
    cellHighPositive: {
      background: '#00ff88',
      color: '#0a0e27',
      fontWeight: 700
    },
    cellHighNegative: {
      background: '#2a2d38',
      color: '#666'
    },
    legend: {
      marginTop: '1.5rem',
      display: 'flex',
      gap: '2rem',
      fontSize: '12px'
    },
    legendItem: {
      display: 'flex',
      alignItems: 'center',
      gap: '8px'
    },
    legendColor: {
      width: '32px',
      height: '20px',
      borderRadius: '4px',
      border: '0.5px solid #1a2547'
    },
    stats: {
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
      gap: '12px',
      marginTop: '1.5rem'
    },
    statCard: {
      background: '#111830',
      border: '1px solid #1a2547',
      borderRadius: '6px',
      padding: '12px',
      textAlign: 'center'
    },
    statLabel: {
      fontSize: '11px',
      color: '#888',
      marginBottom: '6px'
    },
    statValue: {
      fontSize: '18px',
      fontWeight: 700,
      color: '#00ff88'
    }
  };

  const getCellStyle = (value) => {
    const base = { ...styles.cell };
    if (value >= 0.7) return { ...base, ...styles.cellHighPositive };
    if (value >= 0.3) return { ...base, ...styles.cellPositive };
    if (value > -0.3) return { ...base, ...styles.cellNeutral };
    if (value >= -0.7) return { ...base, ...styles.cellNegative };
    return { ...base, ...styles.cellHighNegative };
  };

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <p style={styles.title}>Market Regime Detector</p>
        <p style={styles.subtitle}>EUR/USD Correlation Heatmap</p>
        <p style={styles.timestamp}>
          Updated {timestamp} 
          <span style={{ marginLeft: '12px', color: connected ? '#00ff88' : '#ff4444' }}>
            ● {connected ? 'Live' : 'Disconnected'}
          </span>
        </p>
      </div>

      <div style={styles.heatmapWrapper}>
        <table style={styles.table}>
          <thead>
            <tr>
              <th style={styles.th}></th>
              {instruments.map(instr => (
                <th key={instr} style={styles.th}>{instr}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {instruments.map((row, i) => (
              <tr key={row}>
                <th style={styles.rowHeader}>{row}</th>
                {instruments.map((col, j) => {
                  const value = correlations[row][j];
                  return (
                    <td
                      key={`${row}-${col}`}
                      style={getCellStyle(value)}
                      title={`${row} vs ${col}: ${formatCorr(value)}`}
                    >
                      {formatCorr(value)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div style={styles.legend}>
        <div style={styles.legendItem}>
          <div style={{ ...styles.legendColor, background: 'linear-gradient(135deg, #00ff88 0%, #00cc6f 100%)' }}></div>
          <span style={{ color: '#aaa' }}>Positive correlation (+0.7 to +1.0)</span>
        </div>
        <div style={styles.legendItem}>
          <div style={{ ...styles.legendColor, background: '#3a3f4d' }}></div>
          <span style={{ color: '#aaa' }}>Neutral (-0.3 to +0.3)</span>
        </div>
        <div style={styles.legendItem}>
          <div style={{ ...styles.legendColor, background: '#2a2d38' }}></div>
          <span style={{ color: '#aaa' }}>Negative correlation (-0.7 to -1.0)</span>
        </div>
      </div>

      <div style={styles.stats}>
        <div style={styles.statCard}>
          <div style={styles.statLabel}>Strongest positive</div>
          <div style={styles.statValue}>+0.85</div>
        </div>
        <div style={styles.statCard}>
          <div style={styles.statLabel}>Strongest negative</div>
          <div style={styles.statValue}>-0.78</div>
        </div>
        <div style={styles.statCard}>
          <div style={styles.statLabel}>Active regime</div>
          <div style={styles.statValue}>OIL</div>
        </div>
      </div>
    </div>
  );
};

export default MarketRegimeHeatmap;
