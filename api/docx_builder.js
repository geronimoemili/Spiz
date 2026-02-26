/**
 * docx_builder.js — SPIZ AI
 * Riceve un JSON con report_text, stats, extracted, topic, date
 * Genera un .docx professionale
 * Uso: node docx_builder.js payload.json output.docx
 */

const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  HeadingLevel, AlignmentType, BorderStyle, WidthType, ShadingType,
  Footer, PageBreak, UnderlineType,
} = require('docx');
const fs = require('fs');

// ─── PALETTE ────────────────────────────────────────────────────────
const C = {
  blu:     "1F3A6E",
  bluLt:   "EEF2F8",
  grigio:  "555555",
  griLt:   "F5F5F5",
  nero:    "1A1A1A",
  warn:    "FFF3CD",
  warnBdr: "E6A817",
  bianco:  "FFFFFF",
};

// ─── UTILITY ────────────────────────────────────────────────────────
const bdr1 = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const borders = { top: bdr1, bottom: bdr1, left: bdr1, right: bdr1 };
const noBorders = { top: { style: BorderStyle.NONE }, bottom: { style: BorderStyle.NONE }, left: { style: BorderStyle.NONE }, right: { style: BorderStyle.NONE } };

function sp(n = 1) {
  return Array.from({ length: n }, () =>
    new Paragraph({ spacing: { before: 0, after: 0 }, children: [new TextRun("")] })
  );
}

function h1(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_1,
    spacing: { before: 360, after: 160 },
    border: { bottom: { style: BorderStyle.SINGLE, size: 8, color: C.blu, space: 4 } },
    children: [new TextRun({ text, bold: true, size: 30, color: C.blu, font: "Arial" })]
  });
}

function h2(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_2,
    spacing: { before: 280, after: 100 },
    children: [new TextRun({ text, bold: true, size: 25, color: C.blu, font: "Arial" })]
  });
}

function para(text, opts = {}) {
  return new Paragraph({
    spacing: { before: 80, after: 80, line: 330, lineRule: "auto" },
    children: [new TextRun({ text, size: 22, font: "Arial", color: C.nero, ...opts })]
  });
}

function cite(text) {
  return new Paragraph({
    spacing: { before: 40, after: 40 },
    indent: { left: 560 },
    border: { left: { style: BorderStyle.SINGLE, size: 6, color: C.blu, space: 6 } },
    children: [new TextRun({ text, size: 20, font: "Arial", color: C.grigio, italics: true })]
  });
}

function kv(label, value) {
  return new Paragraph({
    spacing: { before: 70, after: 70 },
    children: [
      new TextRun({ text: label + ": ", size: 22, font: "Arial", bold: true, color: C.blu }),
      new TextRun({ text: value, size: 22, font: "Arial", color: C.nero }),
    ]
  });
}

function infoRow(label, value, bg = C.bluLt) {
  return new Table({
    width: { size: 9026, type: WidthType.DXA },
    columnWidths: [2100, 6926],
    rows: [new TableRow({
      children: [
        new TableCell({
          borders,
          width: { size: 2100, type: WidthType.DXA },
          shading: { fill: C.blu, type: ShadingType.CLEAR },
          margins: { top: 80, bottom: 80, left: 140, right: 140 },
          children: [new Paragraph({ children: [new TextRun({ text: label, size: 20, font: "Arial", color: C.bianco, bold: true })] })]
        }),
        new TableCell({
          borders,
          width: { size: 6926, type: WidthType.DXA },
          shading: { fill: bg, type: ShadingType.CLEAR },
          margins: { top: 80, bottom: 80, left: 140, right: 140 },
          children: [new Paragraph({ children: [new TextRun({ text: value, size: 20, font: "Arial", color: C.nero })] })]
        }),
      ]
    })]
  });
}

// ─── PARSING DEL TESTO REPORT ───────────────────────────────────────
// Converte markdown-lite in elementi docx
function parseReportText(text) {
  const elements = [];
  const lines = text.split('\n');
  for (const line of lines) {
    const l = line.trim();
    if (!l) {
      elements.push(...sp(1));
      continue;
    }
    if (l.startsWith('## ')) {
      elements.push(h1(l.replace(/^##\s*/, '')));
    } else if (l.startsWith('### ')) {
      elements.push(h2(l.replace(/^###\s*/, '')));
    } else if (l.startsWith('- ') || l.startsWith('• ')) {
      const txt = l.replace(/^[-•]\s*/, '');
      elements.push(new Paragraph({
        spacing: { before: 60, after: 60 },
        indent: { left: 480, hanging: 240 },
        children: [
          new TextRun({ text: "• ", size: 22, font: "Arial", color: C.blu, bold: true }),
          new TextRun({ text: txt, size: 22, font: "Arial", color: C.nero }),
        ]
      }));
    } else if (l.startsWith('**') && l.endsWith('**')) {
      elements.push(para(l.replace(/\*\*/g, ''), { bold: true }));
    } else {
      // Riga normale — gestisci **grassetto** inline
      const parts = l.split(/(\*\*[^*]+\*\*)/);
      const runs = parts.map(p => {
        if (p.startsWith('**') && p.endsWith('**')) {
          return new TextRun({ text: p.replace(/\*\*/g, ''), size: 22, font: "Arial", color: C.nero, bold: true });
        }
        return new TextRun({ text: p, size: 22, font: "Arial", color: C.nero });
      });
      elements.push(new Paragraph({
        spacing: { before: 80, after: 80, line: 330, lineRule: "auto" },
        children: runs,
      }));
    }
  }
  return elements;
}

// ─── TABELLA ARTICOLI RILEVANTI ──────────────────────────────────────
function buildArticlesTable(extracted) {
  const top = (extracted || []).filter(a => (a.rilevanza || 0) >= 4).slice(0, 10);
  if (top.length === 0) return [];

  const headerRow = new TableRow({
    tableHeader: true,
    children: [
      new TableCell({
        borders, width: { size: 1200, type: WidthType.DXA },
        shading: { fill: C.blu, type: ShadingType.CLEAR },
        margins: { top: 80, bottom: 80, left: 100, right: 100 },
        children: [new Paragraph({ children: [new TextRun({ text: "Testata", size: 18, font: "Arial", color: C.bianco, bold: true })] })]
      }),
      new TableCell({
        borders, width: { size: 1000, type: WidthType.DXA },
        shading: { fill: C.blu, type: ShadingType.CLEAR },
        margins: { top: 80, bottom: 80, left: 100, right: 100 },
        children: [new Paragraph({ children: [new TextRun({ text: "Data", size: 18, font: "Arial", color: C.bianco, bold: true })] })]
      }),
      new TableCell({
        borders, width: { size: 5026, type: WidthType.DXA },
        shading: { fill: C.blu, type: ShadingType.CLEAR },
        margins: { top: 80, bottom: 80, left: 100, right: 100 },
        children: [new Paragraph({ children: [new TextRun({ text: "Titolo / Nota", size: 18, font: "Arial", color: C.bianco, bold: true })] })]
      }),
      new TableCell({
        borders, width: { size: 800, type: WidthType.DXA },
        shading: { fill: C.blu, type: ShadingType.CLEAR },
        margins: { top: 80, bottom: 80, left: 100, right: 100 },
        children: [new Paragraph({ children: [new TextRun({ text: "Rel.", size: 18, font: "Arial", color: C.bianco, bold: true })] })]
      }),
      new TableCell({
        borders, width: { size: 1000, type: WidthType.DXA },
        shading: { fill: C.blu, type: ShadingType.CLEAR },
        margins: { top: 80, bottom: 80, left: 100, right: 100 },
        children: [new Paragraph({ children: [new TextRun({ text: "Angolo", size: 18, font: "Arial", color: C.bianco, bold: true })] })]
      }),
    ]
  });

  const dataRows = top.map((a, i) => {
    const bg = i % 2 === 0 ? C.bianco : C.griLt;
    const nota = a.nota_redattore || (a.fatti_chiave || []).slice(0,1).join('');
    return new TableRow({
      children: [
        new TableCell({ borders, width: { size: 1200, type: WidthType.DXA }, shading: { fill: bg, type: ShadingType.CLEAR }, margins: { top: 60, bottom: 60, left: 100, right: 100 },
          children: [new Paragraph({ children: [new TextRun({ text: a.testata || '', size: 18, font: "Arial", bold: true, color: C.blu })] })] }),
        new TableCell({ borders, width: { size: 1000, type: WidthType.DXA }, shading: { fill: bg, type: ShadingType.CLEAR }, margins: { top: 60, bottom: 60, left: 100, right: 100 },
          children: [new Paragraph({ children: [new TextRun({ text: a.data || '', size: 18, font: "Arial", color: C.grigio })] })] }),
        new TableCell({ borders, width: { size: 5026, type: WidthType.DXA }, shading: { fill: bg, type: ShadingType.CLEAR }, margins: { top: 60, bottom: 60, left: 100, right: 100 },
          children: [
            new Paragraph({ children: [new TextRun({ text: (a.titolo || '').substring(0, 80), size: 18, font: "Arial", color: C.nero })] }),
            ...(nota ? [new Paragraph({ children: [new TextRun({ text: nota.substring(0, 100), size: 16, font: "Arial", color: C.grigio, italics: true })] })] : []),
          ]}),
        new TableCell({ borders, width: { size: 800, type: WidthType.DXA }, shading: { fill: bg, type: ShadingType.CLEAR }, margins: { top: 60, bottom: 60, left: 100, right: 100 },
          children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [new TextRun({ text: String(a.rilevanza || ''), size: 18, font: "Arial", bold: true, color: C.blu })] })] }),
        new TableCell({ borders, width: { size: 1000, type: WidthType.DXA }, shading: { fill: bg, type: ShadingType.CLEAR }, margins: { top: 60, bottom: 60, left: 100, right: 100 },
          children: [new Paragraph({ children: [new TextRun({ text: a.angolo || '', size: 18, font: "Arial", color: a.angolo === 'critico' || a.angolo === 'negativo' ? C.warnBdr : C.grigio })] })] }),
      ]
    });
  });

  return [
    h2("Articoli di maggiore rilevanza"),
    new Table({
      width: { size: 9026, type: WidthType.DXA },
      columnWidths: [1200, 1000, 5026, 800, 1000],
      rows: [headerRow, ...dataRows],
    }),
    ...sp(1),
  ];
}

// ─── MAIN ────────────────────────────────────────────────────────────
async function main() {
  const payloadPath = process.argv[2];
  const outPath     = process.argv[3];

  if (!payloadPath || !outPath) {
    console.error('Usage: node docx_builder.js payload.json output.docx');
    process.exit(1);
  }

  const payload = JSON.parse(fs.readFileSync(payloadPath, 'utf8'));
  const { report_text, stats, extracted, topic, date: reportDate } = payload;

  const s = stats || {};
  const totale = s.totale || 0;
  const sentimentStr = Object.entries(s.sentiment || {})
    .map(([k,v]) => `${k}: ${v}%`).join(' | ') || 'N/D';
  const testeStr = Object.keys(s.testate || {}).slice(0, 12).join(', ') || 'N/D';
  const periodoStr = s.periodo_da && s.periodo_a
    ? `${s.periodo_da} → ${s.periodo_a}`
    : reportDate || 'N/D';

  // ── COVER ──────────────────────────────────────────────────────────
  const coverElements = [
    new Paragraph({
      alignment: AlignmentType.CENTER,
      spacing: { before: 600, after: 160 },
      children: [new TextRun({ text: (topic || 'REPORT').toUpperCase(), size: 56, bold: true, color: C.blu, font: "Arial" })]
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      spacing: { before: 0, after: 80 },
      children: [new TextRun({ text: "Analisi mediatica — SPIZ AI", size: 26, color: C.grigio, font: "Arial" })]
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      spacing: { before: 0, after: 500 },
      border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: C.blu, space: 6 } },
      children: [new TextRun({ text: `${reportDate || ''}  |  ${totale} articoli analizzati  |  ${periodoStr}`, size: 20, color: C.grigio, font: "Arial" })]
    }),
    ...sp(1),
  ];

  // ── SCHEDA CORPUS ──────────────────────────────────────────────────
  const corpusCard = [
    h1("SCHEDA DEL CORPUS"),
    infoRow("Articoli letti",  String(totale)),
    ...sp(),
    infoRow("Periodo",         periodoStr),
    ...sp(),
    infoRow("Testate",         testeStr),
    ...sp(),
    infoRow("Sentiment",       sentimentStr),
    ...sp(2),
  ];

  // ── CORPO REPORT ───────────────────────────────────────────────────
  const reportElements = parseReportText(report_text || '');

  // ── TABELLA ARTICOLI SALIENTI ──────────────────────────────────────
  const articlesTable = buildArticlesTable(extracted);

  // ── FOOTER ─────────────────────────────────────────────────────────
  const footer = new Footer({
    children: [new Paragraph({
      alignment: AlignmentType.RIGHT,
      children: [new TextRun({ text: `SPIZ AI — MAIM Public Diplomacy & Media Relations  |  ${reportDate || ''}`, size: 18, font: "Arial", color: C.grigio })]
    })]
  });

  // ── DOCUMENT ───────────────────────────────────────────────────────
  const doc = new Document({
    styles: {
      default: { document: { run: { font: "Arial", size: 22 } } },
      paragraphStyles: [
        { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 30, bold: true, font: "Arial", color: C.blu },
          paragraph: { spacing: { before: 360, after: 160 }, outlineLevel: 0 } },
        { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 25, bold: true, font: "Arial", color: C.blu },
          paragraph: { spacing: { before: 280, after: 100 }, outlineLevel: 1 } },
      ]
    },
    sections: [{
      properties: {
        page: {
          size: { width: 11906, height: 16838 },
          margin: { top: 1134, right: 1134, bottom: 1134, left: 1134 }
        }
      },
      footers: { default: footer },
      children: [
        ...coverElements,
        ...corpusCard,
        new Paragraph({ children: [new PageBreak()] }),
        ...reportElements,
        ...sp(2),
        ...articlesTable,
      ]
    }]
  });

  const buf = await Packer.toBuffer(doc);
  fs.writeFileSync(outPath, buf);
  console.log('OK: ' + outPath);
}

main().catch(e => { console.error(e); process.exit(1); });