package trocaBancoPostgres.migrar;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import trocaBancoPostgres.conexao.ConexaoAntiga;
import trocaBancoPostgres.conexao.ConexaoNova;

public class Nota {

	public static void migrarTabela() {
		int count = 0;
		try (Connection remetenteConexao = ConexaoAntiga.obterConexao()) {

			String query = " SELECT id_nota, nota_documento, nota_nota, nota_data, nota_hora, nota_observacao, "
					+ "nota_registro, nota_situacao, nota_chave, nota_total, nota_operacao, nota_usu, "
					+ "id_referencianota, stnota, naturezaint, fornecedorint, modalidade, transportadora, motorista, "
					+ "placa, uf, quantidade, especie, numeracao, pesobruto, pesoliquido, motoristaint, empresaint, "
					+ "datavariavel\n" + "FROM public.nota where stnota=1  order by id_nota asc ;";

			try (Statement sourceStatement = remetenteConexao.createStatement();
					ResultSet resultSet = sourceStatement.executeQuery(query)) {

				while (resultSet.next()) {
					int id = resultSet.getInt("id_nota");
					String nota_documento = resultSet.getString("nota_documento");
					String nota_nota = resultSet.getString("nota_nota");
					String nota_data = resultSet.getString("nota_data");
					String nota_hora = resultSet.getString("nota_hora");
					String nota_observacao = resultSet.getString("nota_observacao");
					String nota_registro = resultSet.getString("nota_registro");
					String nota_situacao = resultSet.getString("nota_situacao");
					String nota_chave = resultSet.getString("nota_chave");
					String nota_operacao = resultSet.getString("nota_operacao");
					int naturezaint = resultSet.getInt("naturezaint");

					String nota_usu = resultSet.getString("nota_usu");
					String id_referencianota = resultSet.getString("id_referencianota");
					String stnota = resultSet.getString("stnota");
					int fornecedorint = resultSet.getInt("fornecedorint");
					String modalidade = resultSet.getString("modalidade");
					String transportadora = resultSet.getString("transportadora");
					String motorista = resultSet.getString("motorista");

					String placa = resultSet.getString("placa");
					String uf = resultSet.getString("uf");
					String quantidade = resultSet.getString("quantidade");
					String especie = resultSet.getString("especie");
					String numeracao = resultSet.getString("numeracao");
					String pesobruto = resultSet.getString("pesobruto");
					String pesoliquido = resultSet.getString("pesoliquido");
					String motoristaint = resultSet.getString("motoristaint");
					String empresaint = resultSet.getString("empresaint");
					String datavariavel = resultSet.getString("datavariavel");

//					System.out.println("--------------------------------------------");
//					System.out.println("id: " + id);
//					System.out.println("nota_documento: " + nota_documento);
//					System.out.println("nota_nota: " + nota_nota);
//					System.out.println("nota_data: " + nota_data);
//					System.out.println("nota_hora: " + nota_hora);
//					System.out.println("nota_observacao: " + nota_observacao);
//					System.out.println("nota_registro: " + nota_registro);
//					System.out.println("nota_situacao: " + nota_situacao);
//					System.out.println("nota_chave: " + nota_chave);
//					System.out.println("nota_usu: " + nota_usu);
//					System.out.println("id_referencianota: " + id_referencianota);
//					System.out.println("stnota: " + stnota);
//					System.out.println("naturezaint: " + naturezaint);
//					System.out.println("fornecedorint: " + fornecedorint);
//					System.out.println("modalidade: " + modalidade);
//					System.out.println("transportadora: " + transportadora);
//					System.out.println("motorista: " + motorista);
//					System.out.println("placa: " + placa);
//					System.out.println("uf: " + uf);
//					System.out.println("quantidade: " + quantidade);
//					System.out.println("especie: " + especie);
//					System.out.println("numeracao: " + numeracao);
//					System.out.println("pesobruto: " + pesobruto);
//					System.out.println("pesoliquido: " + pesoliquido);
//					System.out.println("motoristaint: " + motoristaint);
//					System.out.println("empresaint: " + empresaint);
//					System.out.println("datavariavel: " + datavariavel);
					int tipo = 0;
					if (nota_operacao.equals("SAIDA")) {
						tipo = 1;
					}
					if (nota_hora.trim().equals(":")) {
						nota_hora = "12:00:00";
					}
					if (nota_hora.equals("23:75")) {
						nota_hora = "12:00:00";
					}
					if (nota_hora.equals("12:320")) {
						nota_hora = "12:00:00";
					}
					int cfop = CFOP.encontraCFOP(naturezaint);
					int cliente = Cliente.encontraCliente(fornecedorint);
					System.out.println("id::" + id);
					salvar(id, tipo, cfop, cliente, nota_nota, nota_chave, nota_data, nota_hora, nota_observacao,
							motorista);
					 count++;
				}
			}
			System.out.println("count::" + count);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void salvar(int id, int tipo, int cfop, int cliente, String nota, String chave, String data,
			String hora, String informacao, String motorista) {

		try (Connection novaConexao = ConexaoNova.obterConexao()) {

			String sql = "INSERT INTO transacao (tipo, cfop, cliente, nota, chave, "
					+ "  data_transacao, hora_transacao, informacoes_complementares, deletado,nome_motorista,idAntigo) "
					+ "  VALUES (?, ?, ?, ?, ?, TO_DATE(?, 'DD/MM/YYYY'), TO_TIMESTAMP(?, 'HH24:MI:SS'), ?, false,?,?) ";

			try (PreparedStatement preparedStatement = novaConexao.prepareStatement(sql)) {

				preparedStatement.setInt(1, tipo);
				preparedStatement.setInt(2, cfop);
				preparedStatement.setInt(3, cliente);
				preparedStatement.setString(4, nota);
				preparedStatement.setString(5, chave);
				preparedStatement.setString(6, data);
				preparedStatement.setString(7, hora);
				preparedStatement.setString(8, informacao);
				preparedStatement.setString(9, motorista);
				preparedStatement.setInt(10, id);

				preparedStatement.executeUpdate();
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void dropTable(String tableName) {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "DROP TABLE " + tableName + " CASCADE";
			statement.executeUpdate(sql);

			System.out.println("Tabela " + tableName + " excluída com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void createTable() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "CREATE TABLE transacao (\n" + "    id SERIAL PRIMARY KEY,\n" + "    tipo INTEGER NOT NULL,\n"
					+ "    cfop INTEGER NOT NULL,\n" + "    cliente INTEGER NOT NULL,\n" + "    nota VARCHAR(20),\n"
					+ "    chave VARCHAR(50),\n" + "    data_transacao DATE ,\n" + "    hora_transacao TIME,\n"
					+ "    informacoes_complementares TEXT,\n" + "    deletado BOOLEAN NOT NULL DEFAULT false\n" + ");";

			statement.executeUpdate(sql);

			System.out.println("Tabela Nota criada com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunanomeMotorista() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE transacao\n" + "ADD COLUMN nome_motorista VARCHAR(100);";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'nome_motorista' adicionada à tabela 'Nota' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaStatusNota() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE transacao\n" + "ADD COLUMN status_nota INTEGER NOT NULL DEFAULT 0;";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'status_nota' adicionada à tabela 'Nota' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void alteraColunaChave() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = " ALTER TABLE transacao\n" + "ALTER COLUMN chave TYPE VARCHAR(64);";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'chave' adicionada à tabela 'Nota' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaIdAntigo() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE transacao ADD idAntigo int";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'idAntigo' adicionada à tabela 'transacao' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static int encontraNota(int idAntingo) throws SQLException {
		Connection conn = ConexaoNova.obterConexao();
		int id = 0;
		try {
			String sql = "SELECT id  ativo FROM transacao WHERE idAntigo = ?";
			PreparedStatement preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setInt(1, idAntingo);
			ResultSet resultSet = preparedStatement.executeQuery();
			if (resultSet.next()) {
				id = resultSet.getInt("id");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		conn.close();
		return id;

	}

	public static void main(String[] args) {
		dropTable("Nota");
		createTable();
		adicionarColunanomeMotorista();
		adicionarColunaStatusNota();
		alteraColunaChave();
		adicionarColunaIdAntigo();
		migrarTabela();
	}
}