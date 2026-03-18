import React, { useState, useEffect } from 'react';

const MarketRegimeHeatmap = () => {
  const instruments = ['EUR/USD', 'Oil', 'Gold', 'S&P 500', 'VIX', 'Bund Yield'];
  
  const [correlations, setCorrelations] = useState({
    'EUR/USD': [1.00, 0.52, -0.18, -0.12, -0.45, 0.31],
    'Oil': [0.52, 1.00, 0.25, 0.15, -0.38, -0.08],
    'Gold': [-0.18, 0.25, 1.00, -0.22, 0.68, 0.12],
    'S&P 500': [-0.12, 0.15, -0.22, 1.00, -0.72, -0.35],
    'VIX': [-0.45, -0.38, 0.68, -0.72, 1.00, 0.41],
    'Bund Yield': [0.31, -0.08, 0.12, -0.35, 0.41, 1.00]
  });
  
  const [stats, setStats] = useState({ maxPos: 0.85, maxNeg: -0.78, regime: 'OIL' });
  const [timestamp, setTimestamp] = useState(new Date().toLocaleTimeString());

  useEffect(() => {
    const interval = setInterval(() => {
      setCorrelations(prev => {
        const updated = { ...prev };
        instruments.forEach(instr => {
          updated[instr] = prev[instr].map((val, j) => {
            if (instruments.indexOf(instr) !== j) {
              return Math.max(-0.99, Math.min(0.99, val + (Math.random() - 0.5) * 0.1));
            }
            return val;
          });
        });
        return updated;
      });
      setTimestamp(new Date().toLocaleTimeString());
    }, 3000);

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
        <p style={styles.timestamp}>Updated {timestamp}</p>
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
