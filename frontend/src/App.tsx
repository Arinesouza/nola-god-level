import { useEffect, useState } from "react";

// Tipo do produto
type Produto = {
  id: number;
  nome: string;
  categoria: string;
  preco: number;
  quantidade: number;
};

function App() {
  const [data, setData] = useState<Produto[]>([]);
  const [categoria, setCategoria] = useState<string>("Todas");
  const [searchTerm, setSearchTerm] = useState<string>("");

  useEffect(() => {
    const fetchProdutos = async () => {
      try {
        const res = await fetch("http://localhost:8000/api/produtos");
        if (!res.ok) throw new Error("Erro ao buscar dados do backend");
        const produtos: Produto[] = await res.json();
        setData(produtos.sort((a, b) => a.id - b.id));
      } catch (err: any) {
        console.error("Erro ao buscar dados:", err.message);
      }
    };

    fetchProdutos();
  }, []);

  // Lista de categorias para filtro
  const categorias = ["Todas", ...Array.from(new Set(data.map((d) => d.categoria)))];

  // Filtra por categoria
  let filtrados = categoria === "Todas"
    ? data
    : data.filter((item) => item.categoria === categoria);

  // Filtra por pesquisa
  filtrados = filtrados.filter(
    (item) =>
      item.nome.toLowerCase().includes(searchTerm.toLowerCase()) ||
      item.categoria.toLowerCase().includes(searchTerm.toLowerCase())
  );

  // Métricas
  const totalProdutos = filtrados.length;
  const totalValor = filtrados.reduce((s, i) => s + i.preco * i.quantidade, 0);
  const precoMedio =
    filtrados.length > 0
      ? (filtrados.reduce((s, i) => s + i.preco, 0) / filtrados.length).toFixed(2)
      : "0";

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <h1 className="text-4xl font-bold mb-6 text-slate-800">Dashboard de Produtos</h1>

      {/* Filtros */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4 mb-6">
        <label className="font-medium">
          Categoria:{" "}
          <select
            className="border rounded-md px-2 py-1 ml-2"
            value={categoria}
            onChange={(e) => setCategoria(e.target.value)}
          >
            {categorias.map((c) => (
              <option key={c}>{c}</option>
            ))}
          </select>
        </label>

        <input
          type="text"
          placeholder="Pesquisar por nome ou categoria..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="border rounded-md px-2 py-1 w-full sm:w-64"
        />
      </div>

      {/* Cards de métricas */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-8">
        <div className="bg-white shadow-md rounded-xl p-4 text-center">
          <h2 className="text-gray-500 text-sm">Total de Produtos</h2>
          <p className="text-2xl font-bold text-slate-700">{totalProdutos}</p>
        </div>
        <div className="bg-white shadow-md rounded-xl p-4 text-center">
          <h2 className="text-gray-500 text-sm">Valor Total</h2>
          <p className="text-2xl font-bold text-emerald-600">
            R$ {totalValor.toFixed(2)}
          </p>
        </div>
        <div className="bg-white shadow-md rounded-xl p-4 text-center">
          <h2 className="text-gray-500 text-sm">Preço Médio</h2>
          <p className="text-2xl font-bold text-blue-600">R$ {precoMedio}</p>
        </div>
      </div>

      {/* Tabela responsiva */}
      <div className="overflow-x-auto">
        {filtrados.length === 0 ? (
          <p className="text-gray-600 text-center py-4">Nenhum produto encontrado.</p>
        ) : (
          <table className="min-w-full bg-white rounded-lg shadow-md text-center">
            <thead className="bg-slate-100 text-gray-700 uppercase text-sm sticky top-0">
              <tr>
                <th className="py-3 px-4">ID</th>
                <th className="py-3 px-4">Nome</th>
                <th className="py-3 px-4">Categoria</th>
                <th className="py-3 px-4">Preço</th>
                <th className="py-3 px-4">Quantidade</th>
              </tr>
            </thead>
            <tbody>
              {filtrados.map((item) => (
                <tr key={item.id} className="border-t hover:bg-slate-50 transition-colors">
                  <td className="py-2 px-4">{item.id}</td>
                  <td className="py-2 px-4">{item.nome}</td>
                  <td className="py-2 px-4">{item.categoria}</td>
                  <td className="py-2 px-4">
                    {item.preco.toLocaleString("pt-BR", {
                      style: "currency",
                      currency: "BRL",
                    })}
                  </td>
                  <td className="py-2 px-4">{item.quantidade}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

export default App;
