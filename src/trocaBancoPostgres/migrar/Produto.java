package trocaBancoPostgres.migrar;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import trocaBancoPostgres.conexao.ConexaoAntiga;
import trocaBancoPostgres.conexao.ConexaoNova;

public class Produto {

	public static void migrarTabela() {
		try (Connection remetenteConexao = ConexaoAntiga.obterConexao()) {

			String query = "SELECT id_prod, sis_prod, ncm_prod, tipo_prod, nome_prod, edicao_prod, cfop_prod, saldo_prod, "
					+ "valor_prod, estoque_prod, obs_prod, usu_prod, data_reg, hora_reg, valor_prod_ex, stprod, idunid\n"
					+ "FROM public.produto " + "WHERE stprod=1 ORDER BY  sis_prod ASC;";

			try (Statement sourceStatement = remetenteConexao.createStatement();
					ResultSet resultSet = sourceStatement.executeQuery(query)) {

				while (resultSet.next()) {
					int idUnidade = resultSet.getInt("idunid");
					System.out.println("idUnidade:::" + idUnidade);
					int unidade = Unidade.encontraUnidade(idUnidade);
					System.out.println("unidade:::" + unidade);

					String idNcm = resultSet.getString("ncm_prod");
					idNcm = idNcm.trim();
					System.out.println("idNcm:::" + idNcm);
					if (idNcm.trim().equals("")) {
						System.out.println("erro");
						idNcm = "00000000";
					}
					int ncm = NCM.encontraNCM(idNcm);
					System.out.println("ncm:::" + ncm);

					int id = resultSet.getInt("sis_prod");
					String tipo = resultSet.getString("tipo_prod");
					String nome = resultSet.getString("nome_prod");
					String edicao = resultSet.getString("edicao_prod");
					Double valor = resultSet.getDouble("valor_prod");
					String observacao = resultSet.getString("obs_prod");
					System.out.println("" + id);
					salvar(id, tipo + " " + nome + " " + edicao, valor, observacao, unidade, ncm);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void salvar(int idAntigo, String nome, Double valor, String observacao, int unidade, int ncm) {
		System.out.println("ssalvar::::" + unidade);
		try (Connection novaConexao = ConexaoNova.obterConexao()) {
			String sql = "INSERT INTO produto (descricao, unidade_id, valor, ncm_id, observacao, ativo,idAntigo)"
					+ " VALUES (?, ?, ?, ?, ?, true,?)";

			try (PreparedStatement preparedStatement = novaConexao.prepareStatement(sql)) {
				preparedStatement.setString(1, nome);
				preparedStatement.setInt(2, unidade);
				preparedStatement.setDouble(3, valor);
				preparedStatement.setInt(4, ncm);
				preparedStatement.setString(5, observacao);

                preparedStatement.setInt(6, idAntigo);

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

			String sql = " CREATE TABLE produto (\n" + "    id SERIAL PRIMARY KEY,\n" + "    descricao TEXT NOT NULL,\n"
					+ "    unidade_id INT REFERENCES unidade(id),\n" + "    valor NUMERIC(10, 2) NOT NULL,\n"
					+ "    ncm_id INT REFERENCES ncm(id),\n" + "    observacao TEXT,\n"
					+ "    ativo BOOLEAN DEFAULT true,\n" + "    deletado BOOLEAN DEFAULT false\n" + ");";

			statement.executeUpdate(sql);

			System.out.println("Tabela produto criada com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaDeletado() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE produto ADD deletado BOOLEAN DEFAULT false";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'deletado' adicionada à tabela 'produto' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaIdAntigo() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE produto ADD idAntigo int";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'idAntigo' adicionada à tabela 'produto' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunavaloNumerico() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE produto\n" + "ALTER COLUMN valor TYPE NUMERIC(10,4);";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'valor TYPE NUMERIC(10,4)' adicionada à tabela 'produto' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		dropTable("produto");
		createTable();
		adicionarColunaDeletado();
		adicionarColunavaloNumerico();
		adicionarColunaIdAntigo();
		migrarTabela();
		System.out.println("Finalizado");
	}
}